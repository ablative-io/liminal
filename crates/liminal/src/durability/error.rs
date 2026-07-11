/// Error taxonomy for haematite-backed durability operations.
#[derive(Debug, thiserror::Error)]
pub enum DurabilityError {
    /// Haematite returned a store-level failure.
    ///
    /// The umbrella `From<haematite::ApiError>` conversion lives in
    /// [`super::store`] because it routes the optimistic-concurrency variants to
    /// their dedicated cases rather than wrapping them here.
    #[error("haematite store error: {0}")]
    StoreError(haematite::ApiError),

    /// An append observed a different stream sequence than the caller expected.
    #[error("sequence conflict: expected {expected}, actual {actual}")]
    SequenceConflict {
        /// Caller-provided expected stream sequence.
        expected: u64,
        /// Actual stream sequence reported by haematite.
        actual: u64,
    },

    /// A cursor checkpoint attempted to move from a stale stored value.
    #[error("cursor regression: stored {stored}, attempted {attempted}")]
    CursorRegression {
        /// Current value stored for the cursor.
        stored: u64,
        /// Value the caller attempted to checkpoint from.
        attempted: u64,
    },

    /// A producer idempotency key collided with an existing dedup entry.
    #[error("dedup key collision for key {key}")]
    DedupCollision {
        /// Idempotency key that collided.
        key: String,
    },

    /// Durability configuration failed validation.
    #[error("configuration error: {0}")]
    ConfigError(String),

    /// An ephemeral durable store could not be opened on disk.
    ///
    /// Raised only on the construction path of a self-owned ephemeral store
    /// (see [`super::store::open_ephemeral`]); the guarding temporary directory
    /// is already removed by the time this surfaces.
    #[error("ephemeral store open failed: {0}")]
    EphemeralStoreOpen(String),

    /// An operation reached an ephemeral store whose backing database was
    /// already detached by teardown.
    ///
    /// Unreachable by construction: the store is detached only inside the
    /// ephemeral guard's `Drop`, which cannot overlap a live handle's call.
    /// The variant exists because that detachment is expressed through an
    /// `Option` the compiler cannot see through, and the fallback must be a
    /// typed refusal rather than a panic.
    #[error("ephemeral store already detached by teardown")]
    EphemeralStoreDetached,

    /// Persisted envelope bytes could not be encoded or decoded.
    #[error("envelope serialization error: {0}")]
    EnvelopeError(String),
}

impl From<haematite::SequenceConflict> for DurabilityError {
    fn from(error: haematite::SequenceConflict) -> Self {
        Self::SequenceConflict {
            expected: error.expected,
            actual: error.actual,
        }
    }
}

impl From<haematite::CasMismatch> for DurabilityError {
    fn from(error: haematite::CasMismatch) -> Self {
        // The real `CasMismatch` carries `Option<u64>` to distinguish absent
        // (`None`) from stored-zero (`Some(0)`). The cursor contract treats an
        // absent key as the value 0 (see `HaematiteStore::cas`), so a `None`
        // collapses to 0 here when reporting a regression.
        Self::CursorRegression {
            stored: error.actual.unwrap_or(0),
            attempted: error.expected.unwrap_or(0),
        }
    }
}
