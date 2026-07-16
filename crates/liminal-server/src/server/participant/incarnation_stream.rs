//! Durable connection-incarnation transition-input stream.
//!
//! The log stores canonical inputs, never server-computed allocator snapshots.
//! Cold replay begins at the fixed protocol genesis and re-executes every
//! transition through `liminal-protocol`, so an independently valid but
//! regressing scalar header is not representable. The server owns only event
//! framing, append/flush mechanics, and the log grammar: the first event is a
//! startup, later startups mark later server processes, and allocations are
//! legal only after a startup. Each allocation records the immutable reference
//! bound which validated that input, so later configuration reductions do not
//! reinterpret valid history. All increment, collision, reset, and exhaustion
//! decisions remain in the shared protocol crate.
//!
//! The transition-input format uses the `/v2` stream namespace. The earlier
//! `/v1` checkpoint stored scalar `LPIC` snapshots, so reusing that key for
//! canonical `LPIE` events would make existing durable data ambiguous.
//!
//! Replay advances through bounded pages and stops at the first empty page. It
//! never sleeps, retries, or treats an empty page as a polling signal.

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

const STREAM_KEY: &str = "liminal/participant/incarnation/v2";
const REPLAY_PAGE_SIZE: usize = 256;
const EVENT_MAGIC: [u8; 4] = *b"LPIE";
const EVENT_SCHEMA_VERSION: u8 = 1;
const STARTUP_TAG: u8 = 1;
const ALLOCATE_TAG: u8 = 2;
const EVENT_HEADER_LEN: usize = 10;
const ALLOCATE_BOUND_LEN: usize = 8;
const ALLOCATE_COUNT_LEN: usize = 8;
const ALLOCATE_FIXED_LEN: usize = ALLOCATE_BOUND_LEN + ALLOCATE_COUNT_LEN;
const CONNECTION_INCARNATION_LEN: usize = 16;

const GENESIS_HEADER: ConnectionIncarnationAllocatorRestore =
    ConnectionIncarnationAllocatorRestore {
        server_incarnation: 0,
        last_examined_connection_ordinal: None,
        connection_ordinal_exhausted: false,
    };

/// Failure at the server's durable connection-incarnation seam.
#[derive(Debug, thiserror::Error)]
pub(in crate::server) enum IncarnationStreamError {
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
    /// The canonical event is shorter than the selected fixed field.
    #[error("incarnation event truncated: required {required} bytes, got {actual}")]
    EventTruncated {
        /// Minimum complete byte count at the failing field boundary.
        required: usize,
        /// Complete bytes supplied.
        actual: usize,
    },
    /// A stored event has the wrong canonical schema magic.
    #[error("incarnation event magic mismatch")]
    EventMagic,
    /// A stored event uses an unsupported schema version.
    #[error("unsupported incarnation event schema version {0}")]
    EventSchemaVersion(u8),
    /// A stored event uses an unassigned kind tag.
    #[error("unknown incarnation event kind {0}")]
    EventKind(u8),
    /// The declared event-body length differs from the complete stored suffix.
    #[error("incarnation event body length mismatch: declared {declared}, actual {actual}")]
    EventBodyLength {
        /// Canonical header declaration.
        declared: u32,
        /// Supplied bytes after the fixed event header.
        actual: usize,
    },
    /// A startup event carried a body even though its canonical body is empty.
    #[error("incarnation startup event carried {actual} body bytes")]
    StartupBodyLength {
        /// Nonzero body byte count.
        actual: usize,
    },
    /// An allocate event's count cannot be represented by this process.
    #[error("incarnation allocation reference count {count} exceeds platform usize")]
    ReferenceCountPlatformOverflow {
        /// Canonical u64 reference count.
        count: u64,
    },
    /// An allocate event's immutable reference bound cannot be represented by this process.
    #[error("incarnation allocation reference bound {bound} exceeds platform usize")]
    ReferenceBoundPlatformOverflow {
        /// Canonical u64 reference bound.
        bound: u64,
    },
    /// A stored allocation declares more references than its immutable stored bound.
    #[error("incarnation allocation reference count {actual} exceeds stored bound {maximum}")]
    StoredReferenceCountExceedsBound {
        /// Stored complete-reference count.
        actual: usize,
        /// Immutable complete-reference bound encoded in this event.
        maximum: usize,
    },
    /// An allocate event's count does not select its complete fixed-width suffix.
    #[error(
        "incarnation allocation body length mismatch for {count} references: expected {expected}, actual {actual}"
    )]
    AllocateBodyLength {
        /// Declared complete-reference count.
        count: u64,
        /// Exact body bytes selected by the count.
        expected: usize,
        /// Supplied body bytes.
        actual: usize,
    },
    /// Event framing arithmetic or its u32 body-length conversion overflowed.
    #[error("incarnation event encoded length overflow")]
    EventLengthOverflow,
    /// A durable allocation appeared before the stream's first startup.
    #[error("incarnation allocation event {stored_sequence} precedes first startup")]
    AllocateBeforeStartup {
        /// Durable position carrying the illegal event.
        stored_sequence: u64,
    },
    /// A durable startup event asks the protocol to increment an exhausted server counter.
    #[error("incarnation startup event {stored_sequence} follows server exhaustion")]
    StartupAfterServerExhaustion {
        /// Durable position carrying the illegal event.
        stored_sequence: u64,
    },
    /// A durable allocation event asks the protocol to allocate after terminal exhaustion.
    #[error("incarnation allocation event {stored_sequence} follows ordinal exhaustion")]
    AllocateAfterOrdinalExhaustion {
        /// Durable position carrying the illegal event.
        stored_sequence: u64,
    },
    /// Test-only replay was requested as started but its event history had no startup.
    #[cfg(test)]
    #[error("incarnation event history has no startup")]
    MissingStartup,
    /// The protocol crate rejected a restored allocator genesis or test seed.
    #[error("protocol rejected incarnation allocator header: {0:?}")]
    AllocatorRestore(ConnectionIncarnationAllocatorRestoreError),
    /// The protocol crate rejected an over-bound live complete reference set.
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
pub(in crate::server) enum IncarnationStartup {
    /// The startup input was appended and flushed before this allocator became usable.
    Started(StartedIncarnationStream),
    /// The protocol selected terminal server-incarnation exhaustion.
    Exhausted(ConnectionIncarnationExhausted),
}

/// Persisted connection-ordinal result returned only after its input-event commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::server) enum IncarnationAllocation {
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

#[derive(Debug, PartialEq, Eq)]
enum IncarnationEvent {
    Startup,
    Allocate {
        declared_reference_bound: usize,
        referenced_incarnations: Vec<ConnectionIncarnation>,
    },
}

/// Unstarted handle for the one server-wide append-only incarnation stream.
#[derive(Debug)]
pub(in crate::server) struct IncarnationStream {
    store: Arc<dyn DurableStore>,
    maximum_references: usize,
    #[cfg(test)]
    replay_seed: Option<ConnectionIncarnationAllocatorRestore>,
}

impl IncarnationStream {
    /// Binds the server-wide event stream and complete-reference bound to storage.
    #[must_use]
    pub(in crate::server) const fn new(
        store: Arc<dyn DurableStore>,
        maximum_references: usize,
    ) -> Self {
        Self {
            store,
            maximum_references,
            #[cfg(test)]
            replay_seed: None,
        }
    }

    /// Returns the stable namespaced stream key.
    #[must_use]
    #[cfg(test)]
    pub(in crate::server) const fn stream_key() -> &'static str {
        STREAM_KEY
    }

    /// Builds a test-only stream whose replay begins at a validated high state.
    ///
    /// Production has no raw-header restoration path. This seam lets exhaustion
    /// tests reach `u64::MAX` without appending billions of canonical inputs; all
    /// events after the seed still replay through public protocol transitions.
    #[cfg(test)]
    pub(in crate::server) fn seeded_for_test(
        store: Arc<dyn DurableStore>,
        maximum_references: usize,
        replay_seed: ConnectionIncarnationAllocatorRestore,
    ) -> Result<Self, IncarnationStreamError> {
        let _validated = ConnectionIncarnationAllocator::try_restore(replay_seed)?;
        Ok(Self {
            store,
            maximum_references,
            replay_seed: Some(replay_seed),
        })
    }

    /// Replays canonical inputs, appends this process's startup, and fsyncs it.
    ///
    /// An absent production stream begins at the fixed zero genesis validated by
    /// [`ConnectionIncarnationAllocator::try_restore`]. Append or flush ambiguity
    /// consumes this handle and is never retried here.
    ///
    /// # Errors
    ///
    /// Returns [`IncarnationStreamError`] for malformed events, an illegal event
    /// chain, invalid protocol state, append conflict, or failed flush.
    pub(in crate::server) async fn startup(
        self,
    ) -> Result<IncarnationStartup, IncarnationStreamError> {
        let replayed = self.replay().await?;
        let payload = encode_event(&IncarnationEvent::Startup)?;
        match prepare_server_incarnation_startup(replayed.allocator) {
            ServerIncarnationStartupDecision::Fsync(intent) => {
                let next_sequence =
                    append_and_flush(&self.store, replayed.next_sequence, payload).await?;
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

    async fn replay(&self) -> Result<ReplayedIncarnationState, IncarnationStreamError> {
        let (mut allocator, mut has_started) = self.initial_replay_state()?;
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
                let event = decode_event(&entry.payload)?;
                match event {
                    IncarnationEvent::Startup => {
                        allocator = match prepare_server_incarnation_startup(allocator) {
                            ServerIncarnationStartupDecision::Fsync(intent) => {
                                intent.complete_after_fsync()
                            }
                            ServerIncarnationStartupDecision::Exhausted(_) => {
                                return Err(IncarnationStreamError::StartupAfterServerExhaustion {
                                    stored_sequence: entry.sequence,
                                });
                            }
                        };
                        has_started = true;
                    }
                    IncarnationEvent::Allocate {
                        declared_reference_bound,
                        referenced_incarnations,
                    } => {
                        if !has_started {
                            return Err(IncarnationStreamError::AllocateBeforeStartup {
                                stored_sequence: entry.sequence,
                            });
                        }
                        let references = DurableIncarnationReferences::try_new(
                            &referenced_incarnations,
                            declared_reference_bound,
                        )?;
                        allocator = match allocate_connection_incarnation(allocator, references) {
                            ConnectionIncarnationAllocationDecision::Allocated(allocation) => {
                                allocation.into_resulting()
                            }
                            ConnectionIncarnationAllocationDecision::Exhausted(exhaustion) => {
                                if matches!(
                                    &exhaustion,
                                    ConnectionOrdinalExhaustion::AlreadyExhausted(_)
                                ) {
                                    return Err(
                                        IncarnationStreamError::AllocateAfterOrdinalExhaustion {
                                            stored_sequence: entry.sequence,
                                        },
                                    );
                                }
                                exhaustion.into_resulting()
                            }
                        };
                    }
                }
                next_sequence = next_sequence
                    .checked_add(1)
                    .ok_or(IncarnationStreamError::StreamSequenceExhausted)?;
            }
            if entry_count < REPLAY_PAGE_SIZE {
                break;
            }
        }
        Ok(ReplayedIncarnationState {
            allocator,
            next_sequence,
            #[cfg(test)]
            has_started,
        })
    }

    #[cfg_attr(
        not(test),
        allow(
            clippy::unused_self,
            reason = "the test build reads the validated high-state replay seed from self"
        )
    )]
    fn initial_replay_state(
        &self,
    ) -> Result<(ConnectionIncarnationAllocator, bool), IncarnationStreamError> {
        #[cfg(test)]
        if let Some(seed) = self.replay_seed {
            return Ok((ConnectionIncarnationAllocator::try_restore(seed)?, true));
        }
        Ok((
            ConnectionIncarnationAllocator::try_restore(GENESIS_HEADER)?,
            false,
        ))
    }

    #[cfg(test)]
    pub(in crate::server) async fn resume_started_for_test(
        self,
    ) -> Result<StartedIncarnationStream, IncarnationStreamError> {
        let replayed = self.replay().await?;
        if !replayed.has_started {
            return Err(IncarnationStreamError::MissingStartup);
        }
        Ok(StartedIncarnationStream {
            store: self.store,
            maximum_references: self.maximum_references,
            header: replayed.allocator.as_restore(),
            next_sequence: replayed.next_sequence,
        })
    }
}

/// Started allocator whose latest server-startup input is already durable.
#[derive(Debug)]
pub(in crate::server) struct StartedIncarnationStream {
    store: Arc<dyn DurableStore>,
    maximum_references: usize,
    header: ConnectionIncarnationAllocatorRestore,
    next_sequence: u64,
}

impl StartedIncarnationStream {
    /// Returns the current protocol-derived scalar state for assertions only.
    #[must_use]
    #[cfg(test)]
    pub(super) const fn header(&self) -> ConnectionIncarnationAllocatorRestore {
        self.header
    }

    /// Allocates through the protocol and durably records its complete input.
    ///
    /// The complete reference set is bounded before allocation, encoding, or
    /// append. An already-exhausted decision emits no event. Every successful
    /// allocation and first transition to exhaustion appends and flushes the
    /// exact input before the protocol result becomes live. No failure is retried.
    ///
    /// # Errors
    ///
    /// Returns [`IncarnationStreamError`] for an over-bound reference set,
    /// invalid current state, event-length overflow, append conflict, inconsistent
    /// assigned sequence, or failed flush.
    pub(in crate::server) async fn allocate(
        &mut self,
        referenced_incarnations: &[ConnectionIncarnation],
    ) -> Result<IncarnationAllocation, IncarnationStreamError> {
        let references = DurableIncarnationReferences::try_new(
            referenced_incarnations,
            self.maximum_references,
        )?;
        let event = IncarnationEvent::Allocate {
            declared_reference_bound: self.maximum_references,
            referenced_incarnations: referenced_incarnations.to_vec(),
        };
        let payload = encode_event(&event)?;
        let allocator = ConnectionIncarnationAllocator::try_restore(self.header)?;
        match allocate_connection_incarnation(allocator, references) {
            ConnectionIncarnationAllocationDecision::Allocated(allocation) => {
                let connection_incarnation = allocation.connection_incarnation();
                let skipped_collisions = allocation.skipped_collisions();
                let next_sequence =
                    append_and_flush(&self.store, self.next_sequence, payload).await?;
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
                let next_sequence = if must_append {
                    append_and_flush(&self.store, self.next_sequence, payload).await?
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

#[derive(Debug)]
struct ReplayedIncarnationState {
    allocator: ConnectionIncarnationAllocator,
    next_sequence: u64,
    #[cfg(test)]
    has_started: bool,
}

async fn append_and_flush(
    store: &Arc<dyn DurableStore>,
    expected_sequence: u64,
    payload: Vec<u8>,
) -> Result<u64, IncarnationStreamError> {
    let next_sequence = expected_sequence
        .checked_add(1)
        .ok_or(IncarnationStreamError::StreamSequenceExhausted)?;
    let assigned = store.append(STREAM_KEY, payload, expected_sequence).await?;
    if assigned != expected_sequence {
        return Err(IncarnationStreamError::AssignedSequence {
            expected: expected_sequence,
            actual: assigned,
        });
    }
    store.flush().await?;
    Ok(next_sequence)
}

fn encode_event(event: &IncarnationEvent) -> Result<Vec<u8>, IncarnationStreamError> {
    let body_length = match event {
        IncarnationEvent::Startup => 0,
        IncarnationEvent::Allocate {
            referenced_incarnations,
            ..
        } => referenced_incarnations
            .len()
            .checked_mul(CONNECTION_INCARNATION_LEN)
            .and_then(|references| references.checked_add(ALLOCATE_FIXED_LEN))
            .ok_or(IncarnationStreamError::EventLengthOverflow)?,
    };
    let declared_body_length =
        u32::try_from(body_length).map_err(|_| IncarnationStreamError::EventLengthOverflow)?;
    let total_length = EVENT_HEADER_LEN
        .checked_add(body_length)
        .ok_or(IncarnationStreamError::EventLengthOverflow)?;
    let mut encoded = Vec::with_capacity(total_length);
    encoded.extend_from_slice(&EVENT_MAGIC);
    encoded.push(EVENT_SCHEMA_VERSION);
    encoded.push(match event {
        IncarnationEvent::Startup => STARTUP_TAG,
        IncarnationEvent::Allocate { .. } => ALLOCATE_TAG,
    });
    encoded.extend_from_slice(&declared_body_length.to_be_bytes());
    if let IncarnationEvent::Allocate {
        declared_reference_bound,
        referenced_incarnations,
    } = event
    {
        let declared_reference_bound = u64::try_from(*declared_reference_bound)
            .map_err(|_| IncarnationStreamError::EventLengthOverflow)?;
        let count = u64::try_from(referenced_incarnations.len())
            .map_err(|_| IncarnationStreamError::EventLengthOverflow)?;
        encoded.extend_from_slice(&declared_reference_bound.to_be_bytes());
        encoded.extend_from_slice(&count.to_be_bytes());
        for incarnation in referenced_incarnations {
            encoded.extend_from_slice(&incarnation.server_incarnation.to_be_bytes());
            encoded.extend_from_slice(&incarnation.connection_ordinal.to_be_bytes());
        }
    }
    Ok(encoded)
}

fn decode_event(bytes: &[u8]) -> Result<IncarnationEvent, IncarnationStreamError> {
    if bytes.len() < EVENT_HEADER_LEN {
        return Err(IncarnationStreamError::EventTruncated {
            required: EVENT_HEADER_LEN,
            actual: bytes.len(),
        });
    }
    if bytes.get(..4) != Some(EVENT_MAGIC.as_slice()) {
        return Err(IncarnationStreamError::EventMagic);
    }
    let version = take_u8(bytes, 4)?;
    if version != EVENT_SCHEMA_VERSION {
        return Err(IncarnationStreamError::EventSchemaVersion(version));
    }
    let kind = take_u8(bytes, 5)?;
    let declared_body_length = take_u32(bytes, 6)?;
    let body = bytes
        .get(EVENT_HEADER_LEN..)
        .ok_or(IncarnationStreamError::EventTruncated {
            required: EVENT_HEADER_LEN,
            actual: bytes.len(),
        })?;
    if usize::try_from(declared_body_length)
        .map_err(|_| IncarnationStreamError::EventLengthOverflow)?
        != body.len()
    {
        return Err(IncarnationStreamError::EventBodyLength {
            declared: declared_body_length,
            actual: body.len(),
        });
    }
    match kind {
        STARTUP_TAG if body.is_empty() => Ok(IncarnationEvent::Startup),
        STARTUP_TAG => Err(IncarnationStreamError::StartupBodyLength { actual: body.len() }),
        ALLOCATE_TAG => decode_allocate_event(body),
        unknown => Err(IncarnationStreamError::EventKind(unknown)),
    }
}

fn decode_allocate_event(body: &[u8]) -> Result<IncarnationEvent, IncarnationStreamError> {
    if body.len() < ALLOCATE_FIXED_LEN {
        return Err(IncarnationStreamError::EventTruncated {
            required: EVENT_HEADER_LEN + ALLOCATE_FIXED_LEN,
            actual: EVENT_HEADER_LEN + body.len(),
        });
    }
    let bound = take_u64(body, 0)?;
    let declared_reference_bound = usize::try_from(bound)
        .map_err(|_| IncarnationStreamError::ReferenceBoundPlatformOverflow { bound })?;
    let count = take_u64(body, ALLOCATE_BOUND_LEN)?;
    let count_usize = usize::try_from(count)
        .map_err(|_| IncarnationStreamError::ReferenceCountPlatformOverflow { count })?;
    if count_usize > declared_reference_bound {
        return Err(IncarnationStreamError::StoredReferenceCountExceedsBound {
            actual: count_usize,
            maximum: declared_reference_bound,
        });
    }
    let expected = count_usize
        .checked_mul(CONNECTION_INCARNATION_LEN)
        .and_then(|references| references.checked_add(ALLOCATE_FIXED_LEN))
        .ok_or(IncarnationStreamError::EventLengthOverflow)?;
    if expected != body.len() {
        return Err(IncarnationStreamError::AllocateBodyLength {
            count,
            expected,
            actual: body.len(),
        });
    }
    let mut referenced_incarnations = Vec::with_capacity(count_usize);
    let mut offset = ALLOCATE_FIXED_LEN;
    for _ in 0..count_usize {
        let server_incarnation = take_u64(body, offset)?;
        offset = offset
            .checked_add(8)
            .ok_or(IncarnationStreamError::EventLengthOverflow)?;
        let connection_ordinal = take_u64(body, offset)?;
        offset = offset
            .checked_add(8)
            .ok_or(IncarnationStreamError::EventLengthOverflow)?;
        referenced_incarnations.push(ConnectionIncarnation::new(
            server_incarnation,
            connection_ordinal,
        ));
    }
    Ok(IncarnationEvent::Allocate {
        declared_reference_bound,
        referenced_incarnations,
    })
}

fn take_u8(bytes: &[u8], offset: usize) -> Result<u8, IncarnationStreamError> {
    let required = offset
        .checked_add(1)
        .ok_or(IncarnationStreamError::EventLengthOverflow)?;
    bytes
        .get(offset)
        .copied()
        .ok_or(IncarnationStreamError::EventTruncated {
            required,
            actual: bytes.len(),
        })
}

fn take_u32(bytes: &[u8], offset: usize) -> Result<u32, IncarnationStreamError> {
    let end = offset
        .checked_add(4)
        .ok_or(IncarnationStreamError::EventLengthOverflow)?;
    let encoded: [u8; 4] = bytes
        .get(offset..end)
        .ok_or(IncarnationStreamError::EventTruncated {
            required: end,
            actual: bytes.len(),
        })?
        .try_into()
        .map_err(|_| IncarnationStreamError::EventLengthOverflow)?;
    Ok(u32::from_be_bytes(encoded))
}

fn take_u64(bytes: &[u8], offset: usize) -> Result<u64, IncarnationStreamError> {
    let end = offset
        .checked_add(8)
        .ok_or(IncarnationStreamError::EventLengthOverflow)?;
    let encoded: [u8; 8] = bytes
        .get(offset..end)
        .ok_or(IncarnationStreamError::EventTruncated {
            required: end,
            actual: bytes.len(),
        })?
        .try_into()
        .map_err(|_| IncarnationStreamError::EventLengthOverflow)?;
    Ok(u64::from_be_bytes(encoded))
}

#[cfg(test)]
pub(in crate::server) fn encode_startup_event_fixture() -> Result<Vec<u8>, IncarnationStreamError> {
    encode_event(&IncarnationEvent::Startup)
}

#[cfg(test)]
pub(in crate::server) fn encode_allocate_event_fixture(
    declared_reference_bound: usize,
    referenced_incarnations: &[ConnectionIncarnation],
) -> Result<Vec<u8>, IncarnationStreamError> {
    encode_event(&IncarnationEvent::Allocate {
        declared_reference_bound,
        referenced_incarnations: referenced_incarnations.to_vec(),
    })
}
