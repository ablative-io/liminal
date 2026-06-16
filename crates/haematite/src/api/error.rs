use super::{CasMismatch, SequenceConflict};

/// Errors returned by haematite's event-store API.
#[derive(Debug, thiserror::Error)]
pub enum EventStoreError {
    /// Append failed because the caller's expected sequence was stale.
    #[error(transparent)]
    SequenceConflict(#[from] SequenceConflict),

    /// Compare-and-swap failed because the stored value differed.
    #[error(transparent)]
    CasMismatch(#[from] CasMismatch),

    /// The store could not complete an I/O operation.
    #[error("store I/O error: {0}")]
    StoreIo(#[from] std::io::Error),
}
