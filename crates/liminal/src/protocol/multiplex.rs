use std::{
    collections::{HashMap, hash_map::Entry},
    fmt::Debug,
    ops::{Deref, DerefMut},
};

use super::ProtocolError;

/// Stream identifier carried in every frame header.
///
/// Stream 0 is reserved for connection-level control frames. Application
/// streams use identifiers 1 and above.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StreamId(pub u32);

impl StreamId {
    /// Reserved connection-level control stream.
    pub const CONTROL: Self = Self(0);

    /// Return true when this stream is the reserved control stream.
    #[must_use]
    pub const fn is_control(self) -> bool {
        self.0 == Self::CONTROL.0
    }

    /// Return true when this stream is available for application traffic.
    #[must_use]
    pub const fn is_application(self) -> bool {
        self.0 >= 1
    }
}

/// Lifecycle state tracked for an active stream on a connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamState {
    /// A stream has been opened and is awaiting subscription confirmation.
    Subscribing,
    /// A stream is active and can carry application frames.
    Active,
    /// A stream is closing and awaiting final cleanup.
    Closing,
}

/// Tracks active streams and their current state for a single connection.
#[derive(Debug, Default)]
pub struct StreamTable {
    streams: HashMap<StreamId, StreamState>,
}

impl StreamTable {
    /// Create an empty stream table.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a new active stream in the requested state.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::CodecError`] when the stream already exists in
    /// the table.
    pub fn insert(&mut self, stream_id: StreamId, state: StreamState) -> Result<(), ProtocolError> {
        match self.streams.entry(stream_id) {
            Entry::Vacant(entry) => {
                entry.insert(state);
                Ok(())
            }
            Entry::Occupied(_) => Err(ProtocolError::codec(format!(
                "stream {stream_id:?} already exists"
            ))),
        }
    }

    /// Transition an existing stream to a new state.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::CodecError`] when the stream is not present in
    /// the table.
    pub fn transition(
        &mut self,
        stream_id: StreamId,
        new_state: StreamState,
    ) -> Result<(), ProtocolError> {
        let Some(state) = self.streams.get_mut(&stream_id) else {
            return Err(ProtocolError::codec(format!(
                "stream {stream_id:?} is not active"
            )));
        };

        *state = new_state;
        Ok(())
    }

    /// Remove a stream from the table and return its previous state.
    #[must_use]
    pub fn remove(&mut self, stream_id: StreamId) -> Option<StreamState> {
        self.streams.remove(&stream_id)
    }

    /// Return the tracked state for a stream.
    #[must_use]
    pub fn get(&self, stream_id: StreamId) -> Option<StreamState> {
        self.streams.get(&stream_id).copied()
    }

    /// Return the number of streams currently tracked in the table.
    #[must_use]
    pub fn active_count(&self) -> usize {
        self.streams.len()
    }
}

/// Monotonic stream ID allocator for one side of a connection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamAllocator {
    next_id: Option<u32>,
}

/// Fallible stream ID allocation behavior.
pub trait AllocateStreamId: Debug {
    /// Allocate the next stream ID and advance the allocator.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::CodecError`] when this allocator has exhausted
    /// the `u32` stream ID space for its parity.
    fn next(&mut self) -> Result<StreamId, ProtocolError>;
}

impl StreamAllocator {
    /// Construct a client-side allocator that produces odd stream IDs.
    #[must_use]
    pub const fn client() -> Self {
        Self { next_id: Some(1) }
    }

    /// Construct a server-side allocator that produces even stream IDs.
    #[must_use]
    pub const fn server() -> Self {
        Self { next_id: Some(2) }
    }

    fn allocate_next(&mut self) -> Result<StreamId, ProtocolError> {
        let stream_id = self
            .next_id
            .ok_or_else(|| ProtocolError::codec("stream id space exhausted"))?;
        self.next_id = stream_id.checked_add(2);
        Ok(StreamId(stream_id))
    }
}

impl AllocateStreamId for StreamAllocator {
    fn next(&mut self) -> Result<StreamId, ProtocolError> {
        self.allocate_next()
    }
}

impl Deref for StreamAllocator {
    type Target = dyn AllocateStreamId;

    fn deref(&self) -> &Self::Target {
        self
    }
}

impl DerefMut for StreamAllocator {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::{fmt::Debug, hash::Hash};

    use super::{StreamAllocator, StreamId, StreamState, StreamTable};
    use crate::protocol::ProtocolError;

    #[test]
    fn stream_id_trait_bounds_are_available() {
        fn assert_traits<T: Debug + Clone + Copy + PartialEq + Eq + Hash + PartialOrd + Ord>() {}

        assert_traits::<StreamId>();
    }

    #[test]
    fn stream_zero_is_control_and_not_application() {
        let stream_id = StreamId(0);

        assert!(stream_id.is_control());
        assert!(!stream_id.is_application());
    }

    #[test]
    fn stream_one_is_application_and_not_control() {
        let stream_id = StreamId(1);

        assert!(stream_id.is_application());
        assert!(!stream_id.is_control());
    }

    #[test]
    fn stream_state_has_exact_required_variants() {
        fn state_name(state: StreamState) -> &'static str {
            match state {
                StreamState::Subscribing => "subscribing",
                StreamState::Active => "active",
                StreamState::Closing => "closing",
            }
        }

        let variants = [
            StreamState::Subscribing,
            StreamState::Active,
            StreamState::Closing,
        ];

        assert_eq!(variants.len(), 3);
        assert_eq!(state_name(StreamState::Subscribing), "subscribing");
        assert_eq!(state_name(StreamState::Active), "active");
        assert_eq!(state_name(StreamState::Closing), "closing");
    }

    #[test]
    fn insert_adds_stream_and_counts_it() -> Result<(), ProtocolError> {
        let mut table = StreamTable::new();

        table.insert(StreamId(1), StreamState::Subscribing)?;

        assert_eq!(table.get(StreamId(1)), Some(StreamState::Subscribing));
        assert_eq!(table.active_count(), 1);
        Ok(())
    }

    #[test]
    fn duplicate_insert_returns_error_and_preserves_state() -> Result<(), ProtocolError> {
        let mut table = StreamTable::new();
        table.insert(StreamId(1), StreamState::Subscribing)?;

        let result = table.insert(StreamId(1), StreamState::Active);

        assert!(matches!(result, Err(ProtocolError::CodecError { .. })));
        assert_eq!(table.get(StreamId(1)), Some(StreamState::Subscribing));
        assert_eq!(table.active_count(), 1);
        Ok(())
    }

    #[test]
    fn transition_updates_existing_stream_state() -> Result<(), ProtocolError> {
        let mut table = StreamTable::new();
        table.insert(StreamId(1), StreamState::Subscribing)?;

        table.transition(StreamId(1), StreamState::Active)?;

        assert_eq!(table.get(StreamId(1)), Some(StreamState::Active));
        Ok(())
    }

    #[test]
    fn transition_on_missing_stream_returns_protocol_error() {
        let mut table = StreamTable::new();

        let result = table.transition(StreamId(1), StreamState::Active);

        assert!(matches!(result, Err(ProtocolError::CodecError { .. })));
    }

    #[test]
    fn remove_deletes_stream_and_updates_count() -> Result<(), ProtocolError> {
        let mut table = StreamTable::new();
        table.insert(StreamId(1), StreamState::Active)?;
        table.insert(StreamId(3), StreamState::Closing)?;

        assert_eq!(table.remove(StreamId(1)), Some(StreamState::Active));

        assert_eq!(table.get(StreamId(1)), None);
        assert_eq!(table.active_count(), 1);
        Ok(())
    }

    #[test]
    fn client_allocator_produces_odd_ids() -> Result<(), ProtocolError> {
        let mut allocator = StreamAllocator::client();

        assert_eq!(allocator.next()?, StreamId(1));
        assert_eq!(allocator.next()?, StreamId(3));
        assert_eq!(allocator.next()?, StreamId(5));
        assert_eq!(allocator.next()?, StreamId(7));
        Ok(())
    }

    #[test]
    fn server_allocator_produces_even_ids() -> Result<(), ProtocolError> {
        let mut allocator = StreamAllocator::server();

        assert_eq!(allocator.next()?, StreamId(2));
        assert_eq!(allocator.next()?, StreamId(4));
        assert_eq!(allocator.next()?, StreamId(6));
        assert_eq!(allocator.next()?, StreamId(8));
        Ok(())
    }

    #[test]
    fn client_allocator_errors_after_final_odd_stream_id() -> Result<(), ProtocolError> {
        let mut allocator = StreamAllocator {
            next_id: Some(u32::MAX),
        };

        assert_eq!(allocator.next()?, StreamId(u32::MAX));
        assert!(matches!(
            allocator.next(),
            Err(ProtocolError::CodecError { .. })
        ));
        Ok(())
    }

    #[test]
    fn server_allocator_errors_after_final_even_stream_id() -> Result<(), ProtocolError> {
        let mut allocator = StreamAllocator {
            next_id: Some(u32::MAX - 1),
        };

        assert_eq!(allocator.next()?, StreamId(u32::MAX - 1));
        assert!(matches!(
            allocator.next(),
            Err(ProtocolError::CodecError { .. })
        ));
        Ok(())
    }

    #[test]
    fn allocator_never_recycles_ids() -> Result<(), ProtocolError> {
        let mut allocator = StreamAllocator::client();
        let first = allocator.next()?;
        let second = allocator.next()?;
        let third = allocator.next()?;

        assert!(first < second);
        assert!(second < third);
        Ok(())
    }
}
