//! Durable connection-incarnation allocator stream.
//!
//! This module owns only the append, flush, and bounded cold-replay mechanics.
//! The shared protocol crate validates every restored header and decides the
//! startup increment, collision skips, allocation, and both exhaustion arms.
//! Each mutation appends exactly the canonical scalar header emitted by that
//! protocol transition before its result can be published. Replay advances
//! through bounded pages and stops at the first empty page; it never sleeps or
//! treats an empty page as a polling signal.

use std::sync::Arc;

use liminal::durability::{DurabilityError, DurableStore};
use liminal_protocol::{
    lifecycle::{
        ConnectionIncarnationAllocationDecision, ConnectionIncarnationAllocator,
        ConnectionIncarnationAllocatorRestore, ConnectionIncarnationAllocatorRestoreError,
        ConnectionOrdinalExhaustion, DurableIncarnationReferences,
        DurableIncarnationReferencesError, ServerIncarnationStartupDecision,
        allocate_connection_incarnation, prepare_server_incarnation_startup,
    },
    outcome::ConnectionIncarnationExhausted,
    wire::ConnectionIncarnation,
};

const STREAM_KEY: &str = "liminal/participant/incarnation/v1";
const REPLAY_PAGE_SIZE: usize = 256;
const HEADER_MAGIC: [u8; 4] = *b"LPIC";
const HEADER_SCHEMA_VERSION: u8 = 1;
const HEADER_ENCODED_LEN: usize = 23;

const GENESIS_HEADER: ConnectionIncarnationAllocatorRestore =
    ConnectionIncarnationAllocatorRestore {
        server_incarnation: 0,
        last_examined_connection_ordinal: None,
        connection_ordinal_exhausted: false,
    };

/// Failure at the server's durable connection-incarnation seam.
#[derive(Debug, thiserror::Error)]
pub(super) enum IncarnationStreamError {
    /// The durable engine rejected a read, append, or flush.
    #[error(transparent)]
    Durability(#[from] DurabilityError),
    /// A replay page did not continue at the requested stream sequence.
    #[error("incarnation event sequence mismatch: expected {expected}, got {actual}")]
    EventSequence {
        /// Requested next stream sequence.
        expected: u64,
        /// Sequence returned by durable storage.
        actual: u64,
    },
    /// A successful append returned a sequence other than its optimistic head.
    #[error("incarnation append sequence mismatch: expected {expected}, got {actual}")]
    AssignedSequence {
        /// Optimistic stream head supplied to append.
        expected: u64,
        /// Sequence reported by durable storage.
        actual: u64,
    },
    /// The append-only stream cannot represent another event sequence.
    #[error("incarnation event stream sequence exhausted at u64::MAX")]
    StreamSequenceExhausted,
    /// A stored fixed header has the wrong encoded length.
    #[error("incarnation header length mismatch: expected {expected}, got {actual}")]
    HeaderLength {
        /// Canonical encoded length.
        expected: usize,
        /// Stored encoded length.
        actual: usize,
    },
    /// A stored fixed header has the wrong schema magic.
    #[error("incarnation header magic mismatch")]
    HeaderMagic,
    /// A stored fixed header has an unsupported schema version.
    #[error("unsupported incarnation header schema version {0}")]
    HeaderSchemaVersion(u8),
    /// A stored fixed header contains a non-canonical scalar flag or padding.
    #[error("non-canonical incarnation header field {field}")]
    HeaderScalar {
        /// Fixed field whose encoding was invalid.
        field: &'static str,
    },
    /// The protocol crate rejected a restored allocator header.
    #[error("protocol rejected incarnation allocator header: {0:?}")]
    AllocatorRestore(ConnectionIncarnationAllocatorRestoreError),
    /// The protocol crate rejected an over-bound complete reference set.
    #[error("protocol rejected durable incarnation references: {0:?}")]
    DurableReferences(DurableIncarnationReferencesError),
}

impl From<ConnectionIncarnationAllocatorRestoreError> for IncarnationStreamError {
    fn from(error: ConnectionIncarnationAllocatorRestoreError) -> Self {
        Self::AllocatorRestore(error)
    }
}

impl From<DurableIncarnationReferencesError> for IncarnationStreamError {
    fn from(error: DurableIncarnationReferencesError) -> Self {
        Self::DurableReferences(error)
    }
}

/// Cold-start result after applying the protocol-owned server increment.
#[derive(Debug)]
pub(super) enum IncarnationStartup {
    /// The incremented header was appended and flushed before this allocator
    /// became usable.
    Started(StartedIncarnationStream),
    /// The protocol selected terminal server-incarnation exhaustion.
    Exhausted(ConnectionIncarnationExhausted),
}

/// Persisted connection-ordinal result returned only after its header commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum IncarnationAllocation {
    /// One collision-free pair was durably allocated.
    Allocated {
        /// Exact protocol-emitted pair safe to publish.
        connection_incarnation: ConnectionIncarnation,
        /// Number of exact durable collisions skipped by the protocol.
        skipped_collisions: usize,
    },
    /// The protocol selected stable connection-ordinal exhaustion.
    Exhausted(ConnectionIncarnationExhausted),
}

/// Unstarted handle for the one server-wide append-only incarnation stream.
#[derive(Debug)]
pub(super) struct IncarnationStream {
    store: Arc<dyn DurableStore>,
    maximum_references: usize,
}

impl IncarnationStream {
    /// Binds the server-wide stream and configured complete-reference bound to
    /// durable storage.
    #[must_use]
    pub(super) const fn new(store: Arc<dyn DurableStore>, maximum_references: usize) -> Self {
        Self {
            store,
            maximum_references,
        }
    }

    /// Returns the stable server-wide stream key.
    #[must_use]
    #[cfg(test)]
    pub(super) const fn stream_key() -> &'static str {
        STREAM_KEY
    }

    /// Replays bounded pages, applies the protocol startup decision, and fsyncs
    /// a successful increment before returning a started allocator.
    ///
    /// An absent stream is the storage bootstrap state whose raw zero header is
    /// still validated by [`ConnectionIncarnationAllocator::try_restore`]. The
    /// first persisted value is therefore the protocol-emitted checked startup
    /// increment, never a server-computed successor.
    ///
    /// # Errors
    ///
    /// Returns [`IncarnationStreamError`] for malformed durable bytes, a stream
    /// gap, invalid protocol state, append conflict, or failed flush. On append
    /// or flush ambiguity the caller must discard this consumed unstarted
    /// handle; this API performs no retry.
    pub(super) async fn startup(self) -> Result<IncarnationStartup, IncarnationStreamError> {
        let replayed = self.replay().await?;
        let allocator = ConnectionIncarnationAllocator::try_restore(replayed.header)?;
        match prepare_server_incarnation_startup(allocator) {
            ServerIncarnationStartupDecision::Fsync(intent) => {
                let emitted_header = intent.header_to_fsync();
                let next_sequence =
                    append_and_flush(&self.store, replayed.next_sequence, emitted_header).await?;
                let completed = intent.complete_after_fsync();
                Ok(IncarnationStartup::Started(StartedIncarnationStream {
                    store: self.store,
                    maximum_references: self.maximum_references,
                    header: completed.as_restore(),
                    next_sequence,
                }))
            }
            ServerIncarnationStartupDecision::Exhausted(exhausted) => {
                Ok(IncarnationStartup::Exhausted(exhausted.outcome()))
            }
        }
    }

    async fn replay(&self) -> Result<ReplayedIncarnationHeader, IncarnationStreamError> {
        let mut header = GENESIS_HEADER;
        let mut next_sequence = 0_u64;
        loop {
            let entries = self
                .store
                .read_from(STREAM_KEY, next_sequence, REPLAY_PAGE_SIZE)
                .await?;
            if entries.is_empty() {
                break;
            }
            let entry_count = entries.len();
            for entry in entries {
                if entry.sequence != next_sequence {
                    return Err(IncarnationStreamError::EventSequence {
                        expected: next_sequence,
                        actual: entry.sequence,
                    });
                }
                header = decode_header(&entry.payload)?;
                let _validated = ConnectionIncarnationAllocator::try_restore(header)?;
                next_sequence = next_sequence
                    .checked_add(1)
                    .ok_or(IncarnationStreamError::StreamSequenceExhausted)?;
            }
            if entry_count < REPLAY_PAGE_SIZE {
                break;
            }
        }
        Ok(ReplayedIncarnationHeader {
            header,
            next_sequence,
        })
    }

    #[cfg(test)]
    pub(super) async fn resume_started_for_test(
        self,
    ) -> Result<StartedIncarnationStream, IncarnationStreamError> {
        let replayed = self.replay().await?;
        let allocator = ConnectionIncarnationAllocator::try_restore(replayed.header)?;
        Ok(StartedIncarnationStream {
            store: self.store,
            maximum_references: self.maximum_references,
            header: allocator.as_restore(),
            next_sequence: replayed.next_sequence,
        })
    }
}

/// Started allocator whose server-incarnation increment is already durable.
#[derive(Debug)]
pub(super) struct StartedIncarnationStream {
    store: Arc<dyn DurableStore>,
    maximum_references: usize,
    header: ConnectionIncarnationAllocatorRestore,
    next_sequence: u64,
}

impl StartedIncarnationStream {
    /// Returns the current protocol-emitted durable scalar header.
    #[must_use]
    pub(super) const fn header(&self) -> ConnectionIncarnationAllocatorRestore {
        self.header
    }

    /// Allocates through the protocol using a complete bounded reference set,
    /// then appends and flushes any emitted header before returning its result.
    ///
    /// An already-exhausted replay emits no new header and performs no append.
    /// A first collision at ordinal `u64::MAX` appends the protocol-emitted
    /// terminal header before returning exhaustion. No conflict or durability
    /// error is retried here.
    ///
    /// # Errors
    ///
    /// Returns [`IncarnationStreamError`] for an over-bound reference set,
    /// invalid current header, append conflict, inconsistent assigned sequence,
    /// or failed flush. After append or flush ambiguity the caller must discard
    /// this handle and re-enter through the process's recovery boundary.
    pub(super) async fn allocate(
        &mut self,
        referenced_incarnations: &[ConnectionIncarnation],
    ) -> Result<IncarnationAllocation, IncarnationStreamError> {
        let references = DurableIncarnationReferences::try_new(
            referenced_incarnations,
            self.maximum_references,
        )?;
        let allocator = ConnectionIncarnationAllocator::try_restore(self.header)?;
        match allocate_connection_incarnation(allocator, references) {
            ConnectionIncarnationAllocationDecision::Allocated(allocation) => {
                let connection_incarnation = allocation.connection_incarnation();
                let skipped_collisions = allocation.skipped_collisions();
                let emitted_header = allocation.resulting_header();
                let next_sequence =
                    append_and_flush(&self.store, self.next_sequence, emitted_header).await?;
                let resulting = allocation.into_resulting();
                self.header = resulting.as_restore();
                self.next_sequence = next_sequence;
                Ok(IncarnationAllocation::Allocated {
                    connection_incarnation,
                    skipped_collisions,
                })
            }
            ConnectionIncarnationAllocationDecision::Exhausted(exhaustion) => {
                let outcome = exhaustion.outcome();
                let must_append =
                    matches!(&exhaustion, ConnectionOrdinalExhaustion::MarkExhausted(_));
                let emitted_header = exhaustion.resulting_header();
                let next_sequence = if must_append {
                    append_and_flush(&self.store, self.next_sequence, emitted_header).await?
                } else {
                    self.next_sequence
                };
                let resulting = exhaustion.into_resulting();
                self.header = resulting.as_restore();
                self.next_sequence = next_sequence;
                Ok(IncarnationAllocation::Exhausted(outcome))
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct ReplayedIncarnationHeader {
    header: ConnectionIncarnationAllocatorRestore,
    next_sequence: u64,
}

async fn append_and_flush(
    store: &Arc<dyn DurableStore>,
    expected_sequence: u64,
    header: ConnectionIncarnationAllocatorRestore,
) -> Result<u64, IncarnationStreamError> {
    let next_sequence = expected_sequence
        .checked_add(1)
        .ok_or(IncarnationStreamError::StreamSequenceExhausted)?;
    let assigned = store
        .append(STREAM_KEY, encode_header(header), expected_sequence)
        .await?;
    if assigned != expected_sequence {
        return Err(IncarnationStreamError::AssignedSequence {
            expected: expected_sequence,
            actual: assigned,
        });
    }
    store.flush().await?;
    Ok(next_sequence)
}

fn encode_header(header: ConnectionIncarnationAllocatorRestore) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(HEADER_ENCODED_LEN);
    bytes.extend_from_slice(&HEADER_MAGIC);
    bytes.push(HEADER_SCHEMA_VERSION);
    bytes.extend_from_slice(&header.server_incarnation.to_be_bytes());
    if let Some(ordinal) = header.last_examined_connection_ordinal {
        bytes.push(1);
        bytes.extend_from_slice(&ordinal.to_be_bytes());
    } else {
        bytes.push(0);
        bytes.extend_from_slice(&0_u64.to_be_bytes());
    }
    bytes.push(u8::from(header.connection_ordinal_exhausted));
    bytes
}

fn decode_header(
    bytes: &[u8],
) -> Result<ConnectionIncarnationAllocatorRestore, IncarnationStreamError> {
    if bytes.len() != HEADER_ENCODED_LEN {
        return Err(IncarnationStreamError::HeaderLength {
            expected: HEADER_ENCODED_LEN,
            actual: bytes.len(),
        });
    }
    if bytes.get(..4) != Some(HEADER_MAGIC.as_slice()) {
        return Err(IncarnationStreamError::HeaderMagic);
    }
    if bytes[4] != HEADER_SCHEMA_VERSION {
        return Err(IncarnationStreamError::HeaderSchemaVersion(bytes[4]));
    }
    let server_incarnation = decode_u64(bytes, 5);
    let ordinal_value = decode_u64(bytes, 14);
    let last_examined_connection_ordinal = match bytes[13] {
        0 if ordinal_value == 0 => None,
        0 => {
            return Err(IncarnationStreamError::HeaderScalar {
                field: "absent_connection_ordinal_padding",
            });
        }
        1 => Some(ordinal_value),
        _ => {
            return Err(IncarnationStreamError::HeaderScalar {
                field: "connection_ordinal_presence",
            });
        }
    };
    let connection_ordinal_exhausted = match bytes[22] {
        0 => false,
        1 => true,
        _ => {
            return Err(IncarnationStreamError::HeaderScalar {
                field: "connection_ordinal_exhausted",
            });
        }
    };
    Ok(ConnectionIncarnationAllocatorRestore {
        server_incarnation,
        last_examined_connection_ordinal,
        connection_ordinal_exhausted,
    })
}

fn decode_u64(bytes: &[u8], start: usize) -> u64 {
    let mut encoded = [0_u8; 8];
    encoded.copy_from_slice(&bytes[start..start + 8]);
    u64::from_be_bytes(encoded)
}

#[cfg(test)]
pub(super) fn encode_header_fixture(header: ConnectionIncarnationAllocatorRestore) -> Vec<u8> {
    encode_header(header)
}
