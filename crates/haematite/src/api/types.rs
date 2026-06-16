/// Stored event payload and metadata in a haematite event stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    /// Opaque payload bytes stored for the event.
    pub payload: Vec<u8>,
    /// Assigned sequence number within the event stream.
    pub sequence: u64,
    /// Millisecond Unix timestamp captured when the event was appended.
    pub timestamp: u64,
}

impl Event {
    /// Creates a stored event from its required fields.
    #[must_use]
    pub fn new(payload: Vec<u8>, sequence: u64, timestamp: u64) -> Self {
        Self {
            payload,
            sequence,
            timestamp,
        }
    }
}

/// Optimistic-concurrency failure for an event-stream append.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("sequence conflict: expected {expected}, actual {actual}")]
pub struct SequenceConflict {
    /// Caller-provided expected next sequence.
    pub expected: u64,
    /// Actual next sequence stored for the stream.
    pub actual: u64,
}

/// Compare-and-swap mismatch for a stored numeric value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("compare-and-swap mismatch: expected {expected}, actual {actual}")]
pub struct CasMismatch {
    /// Caller-provided expected value.
    pub expected: u64,
    /// Actual stored value.
    pub actual: u64,
}
