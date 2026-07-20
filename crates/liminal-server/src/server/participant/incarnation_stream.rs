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

use std::{collections::BTreeMap, sync::Arc};

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
const FROZEN_EVENT_SCHEMA_VERSION: u8 = 1;
const EVENT_SCHEMA_VERSION: u8 = 2;
const STARTUP_TAG: u8 = 1;
const ALLOCATE_TAG: u8 = 2;
const OPEN_CONNECTION_FATE_TAG: u8 = 3;
const COMPLETE_CONNECTION_FATE_TAG: u8 = 4;
const EVENT_HEADER_LEN: usize = 10;
const ALLOCATE_BOUND_LEN: usize = 8;
const ALLOCATE_COUNT_LEN: usize = 8;
const ALLOCATE_FIXED_LEN: usize = ALLOCATE_BOUND_LEN + ALLOCATE_COUNT_LEN;
const CONNECTION_INCARNATION_LEN: usize = 16;
const CONNECTION_FATE_CLASS_LEN: usize = 1;
const CONVERSATION_BOUND_LEN: usize = 8;
const CONVERSATION_COUNT_LEN: usize = 8;
const CONVERSATION_ID_LEN: usize = 8;
const OPEN_CONNECTION_FATE_FIXED_LEN: usize = CONNECTION_INCARNATION_LEN
    + CONNECTION_FATE_CLASS_LEN
    + CONVERSATION_BOUND_LEN
    + CONVERSATION_COUNT_LEN;
const COMPLETE_CONNECTION_FATE_LEN: usize = 8;

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
    /// A v1 event used a kind introduced by the v2 grammar.
    #[error("incarnation event kind {kind} requires schema version 2, got {version}")]
    EventKindSchemaVersion {
        /// Stored schema version.
        version: u8,
        /// Stored event-kind tag.
        kind: u8,
    },
    /// An Open's conversation count cannot be represented by this process.
    #[error("connection-fate conversation count {count} exceeds platform usize")]
    ConversationCountPlatformOverflow {
        /// Canonical u64 conversation count.
        count: u64,
    },
    /// An Open's declared conversation bound cannot be represented by this process.
    #[error("connection-fate conversation bound {bound} exceeds platform usize")]
    ConversationBoundPlatformOverflow {
        /// Canonical u64 conversation bound.
        bound: u64,
    },
    /// An Open contains more conversations than its signed declared bound.
    #[error("connection-fate conversation count {actual} exceeds declared bound {maximum}")]
    ConversationCountExceedsBound {
        /// Complete conversation count.
        actual: usize,
        /// Immutable signed bound stored in this Open.
        maximum: usize,
    },
    /// An Open's count does not select its complete fixed-width suffix.
    #[error(
        "connection-fate Open body length mismatch for {count} conversations: expected {expected}, actual {actual}"
    )]
    OpenConnectionFateBodyLength {
        /// Declared complete-conversation count.
        count: u64,
        /// Exact body bytes selected by the count.
        expected: usize,
        /// Supplied body bytes.
        actual: usize,
    },
    /// An Open's conversation ids are not canonical sorted-set bytes.
    #[error("connection-fate Open conversation ids are not strictly increasing at index {index}")]
    ConversationsNotStrictlyIncreasing {
        /// Index of the duplicate or regressing id.
        index: usize,
    },
    /// An Open carries an unassigned typed fate class.
    #[error("unknown connection-fate class {0}")]
    ConnectionFateClass(u8),
    /// A Complete body is not its one exact u64 Open sequence.
    #[error("connection-fate Complete body length mismatch: expected 8, actual {actual}")]
    CompleteConnectionFateBodyLength {
        /// Supplied body bytes.
        actual: usize,
    },
    /// Two Allocate inputs in one Startup generation persisted different bounds.
    #[error(
        "startup generation {startup_sequence} allocation {stored_sequence} changed declared reference bound from {expected} to {actual}"
    )]
    GenerationReferenceBoundConflict {
        startup_sequence: u64,
        expected: usize,
        actual: usize,
        stored_sequence: u64,
    },
    /// An Open names no retained Allocate output in its enclosing generation.
    #[error(
        "connection-fate Open {open_sequence} names unknown allocation {connection_incarnation:?}"
    )]
    UnknownConnectionAllocation {
        open_sequence: u64,
        connection_incarnation: ConnectionIncarnation,
    },
    /// Simultaneous Opens resolved to different Startup generations.
    #[error(
        "connection-fate Open {open_sequence} resolves to startup generation {actual_startup_sequence}, but unmatched Opens belong to {expected_startup_sequence}"
    )]
    CrossGenerationOpen {
        open_sequence: u64,
        expected_startup_sequence: u64,
        actual_startup_sequence: u64,
    },
    /// One connection has two simultaneously unmatched Opens.
    #[error(
        "connection-fate Open {open_sequence} duplicates connection from unmatched Open {existing_open_sequence}"
    )]
    DuplicateConnectionOpen {
        open_sequence: u64,
        existing_open_sequence: u64,
    },
    /// A Complete names no currently unmatched Open.
    #[error("connection-fate Complete {complete_sequence} names absent Open {open_sequence}")]
    CompleteForAbsentOpen {
        complete_sequence: u64,
        open_sequence: u64,
    },
    /// An unmatched set exceeds its selected generation's persisted bound.
    #[error(
        "startup generation {startup_sequence} connection-fate active count {actual} exceeds persisted bound {bound} at Open {open_sequence}"
    )]
    HistoricalConnectionFateBoundExceeded {
        startup_sequence: u64,
        bound: usize,
        actual: usize,
        open_sequence: u64,
    },
    /// Recovery tried to append a Complete for no replay-retained Open.
    #[cfg(test)]
    #[error("connection-fate recovery names absent Open {open_sequence}")]
    RecoveryCompleteForAbsentOpen { open_sequence: u64 },
    /// Startup recovery cannot finish while durable Opens remain unmatched.
    #[cfg(test)]
    #[error("connection-fate recovery still has {count} unmatched Opens")]
    RecoveryIncomplete { count: usize },
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
    /// Test-only resume refuses to discard unmatched durable work.
    #[cfg(test)]
    #[error("incarnation event history retains {count} unmatched connection-fate Opens")]
    UnmatchedConnectionFates { count: usize },
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
    /// Unmatched historical Opens must Complete before a new Startup can append.
    RecoveryRequired(ConnectionFateRecovery),
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

/// Exact transport/server classification retained by one durable connection-fate Open.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::server) enum ConnectionFateClass {
    /// A protocol-level clean Disconnect.
    CleanDisconnect,
    /// An orderly server `ForceClose`.
    ServerShutdown,
    /// EOF or transport loss without clean protocol evidence.
    ConnectionLost,
    /// A terminal protocol/decode refusal after participant binding.
    ProtocolError,
}

impl ConnectionFateClass {
    const fn tag(self) -> u8 {
        match self {
            Self::CleanDisconnect => 1,
            Self::ServerShutdown => 2,
            Self::ConnectionLost => 3,
            Self::ProtocolError => 4,
        }
    }

    const fn from_tag(tag: u8) -> Result<Self, IncarnationStreamError> {
        match tag {
            1 => Ok(Self::CleanDisconnect),
            2 => Ok(Self::ServerShutdown),
            3 => Ok(Self::ConnectionLost),
            4 => Ok(Self::ProtocolError),
            unknown => Err(IncarnationStreamError::ConnectionFateClass(unknown)),
        }
    }
}

/// One replay-validated durable connection-fate work item.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::server) struct ConnectionFateIntent {
    pub(in crate::server) open_sequence: u64,
    pub(in crate::server) allocation_sequence: u64,
    pub(in crate::server) startup_sequence: u64,
    pub(in crate::server) server_incarnation: u64,
    pub(in crate::server) declared_reference_bound: usize,
    pub(in crate::server) connection_incarnation: ConnectionIncarnation,
    pub(in crate::server) class: ConnectionFateClass,
    pub(in crate::server) declared_conversation_bound: usize,
    pub(in crate::server) conversations: Vec<u64>,
}

#[derive(Debug)]
struct AllocationGeneration {
    startup_sequence: u64,
    server_incarnation: u64,
    declared_reference_bound: Option<usize>,
    active_allocations: BTreeMap<ConnectionIncarnation, u64>,
}

impl AllocationGeneration {
    const fn new(startup_sequence: u64, server_incarnation: u64) -> Self {
        Self {
            startup_sequence,
            server_incarnation,
            declared_reference_bound: None,
            active_allocations: BTreeMap::new(),
        }
    }

    const fn establish_bound(
        &mut self,
        stored_sequence: u64,
        declared_reference_bound: usize,
    ) -> Result<(), IncarnationStreamError> {
        if let Some(expected) = self.declared_reference_bound
            && expected != declared_reference_bound
        {
            return Err(IncarnationStreamError::GenerationReferenceBoundConflict {
                startup_sequence: self.startup_sequence,
                expected,
                actual: declared_reference_bound,
                stored_sequence,
            });
        }
        self.declared_reference_bound = Some(declared_reference_bound);
        Ok(())
    }
}

#[derive(Debug, Default)]
struct UnmatchedConnectionFates {
    generation_startup_sequence: Option<u64>,
    by_sequence: BTreeMap<u64, ConnectionFateIntent>,
}

impl UnmatchedConnectionFates {
    fn open(
        &mut self,
        open_sequence: u64,
        generation: &AllocationGeneration,
        connection_incarnation: ConnectionIncarnation,
        class: ConnectionFateClass,
        declared_conversation_bound: usize,
        conversations: Vec<u64>,
    ) -> Result<(), IncarnationStreamError> {
        let Some(&allocation_sequence) = generation.active_allocations.get(&connection_incarnation)
        else {
            return Err(IncarnationStreamError::UnknownConnectionAllocation {
                open_sequence,
                connection_incarnation,
            });
        };
        if let Some(expected_startup_sequence) = self.generation_startup_sequence
            && expected_startup_sequence != generation.startup_sequence
        {
            return Err(IncarnationStreamError::CrossGenerationOpen {
                open_sequence,
                expected_startup_sequence,
                actual_startup_sequence: generation.startup_sequence,
            });
        }
        if let Some(existing) = self
            .by_sequence
            .values()
            .find(|intent| intent.connection_incarnation == connection_incarnation)
        {
            return Err(IncarnationStreamError::DuplicateConnectionOpen {
                open_sequence,
                existing_open_sequence: existing.open_sequence,
            });
        }
        let declared_reference_bound = generation.declared_reference_bound.ok_or(
            IncarnationStreamError::UnknownConnectionAllocation {
                open_sequence,
                connection_incarnation,
            },
        )?;
        let active_count = self
            .by_sequence
            .len()
            .checked_add(1)
            .ok_or(IncarnationStreamError::EventLengthOverflow)?;
        if active_count > declared_reference_bound {
            return Err(
                IncarnationStreamError::HistoricalConnectionFateBoundExceeded {
                    startup_sequence: generation.startup_sequence,
                    bound: declared_reference_bound,
                    actual: active_count,
                    open_sequence,
                },
            );
        }
        self.generation_startup_sequence = Some(generation.startup_sequence);
        self.by_sequence.insert(
            open_sequence,
            ConnectionFateIntent {
                open_sequence,
                allocation_sequence,
                startup_sequence: generation.startup_sequence,
                server_incarnation: generation.server_incarnation,
                declared_reference_bound,
                connection_incarnation,
                class,
                declared_conversation_bound,
                conversations,
            },
        );
        Ok(())
    }

    fn complete(
        &mut self,
        complete_sequence: u64,
        open_sequence: u64,
    ) -> Result<(), IncarnationStreamError> {
        if self.by_sequence.remove(&open_sequence).is_none() {
            return Err(IncarnationStreamError::CompleteForAbsentOpen {
                complete_sequence,
                open_sequence,
            });
        }
        if self.by_sequence.is_empty() {
            self.generation_startup_sequence = None;
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq)]
enum IncarnationEvent {
    Startup,
    Allocate {
        declared_reference_bound: usize,
        referenced_incarnations: Vec<ConnectionIncarnation>,
    },
    OpenConnectionFate {
        connection_incarnation: ConnectionIncarnation,
        class: ConnectionFateClass,
        declared_conversation_bound: usize,
        conversations: Vec<u64>,
    },
    CompleteConnectionFate {
        open_event_sequence: u64,
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
        if !replayed.unmatched.by_sequence.is_empty() {
            return Ok(IncarnationStartup::RecoveryRequired(
                ConnectionFateRecovery {
                    #[cfg(test)]
                    store: self.store,
                    #[cfg(test)]
                    maximum_references: self.maximum_references,
                    #[cfg(test)]
                    allocator: replayed.allocator,
                    #[cfg(test)]
                    next_sequence: replayed.next_sequence,
                    unmatched: replayed.unmatched,
                },
            ));
        }
        let payload = encode_event(&IncarnationEvent::Startup)?;
        let startup_sequence = replayed.next_sequence;
        match prepare_server_incarnation_startup(replayed.allocator) {
            ServerIncarnationStartupDecision::Fsync(intent) => {
                let next_sequence =
                    append_and_flush(&self.store, startup_sequence, payload).await?;
                let completed = intent.complete_after_fsync();
                let header = completed.as_restore();
                Ok(IncarnationStartup::Started(StartedIncarnationStream {
                    store: self.store,
                    maximum_references: self.maximum_references,
                    header,
                    next_sequence,
                    generation: AllocationGeneration::new(
                        startup_sequence,
                        header.server_incarnation,
                    ),
                }))
            }
            ServerIncarnationStartupDecision::Exhausted(exhausted) => {
                Ok(IncarnationStartup::Exhausted(exhausted.outcome()))
            }
        }
    }

    async fn replay(&self) -> Result<ReplayedIncarnationState, IncarnationStreamError> {
        let (allocator, has_started) = self.initial_replay_state()?;
        let mut next_sequence = 0_u64;
        let generation = has_started.then(|| {
            AllocationGeneration::new(next_sequence, allocator.as_restore().server_incarnation)
        });
        let mut replay = ReplayAccumulator {
            allocator,
            generation,
            unmatched: UnmatchedConnectionFates::default(),
            has_started,
        };
        loop {
            let entries = self
                .store
                .read_from(STREAM_KEY, next_sequence, REPLAY_PAGE_SIZE)
                .await?;
            if entries.is_empty() {
                break;
            }
            for entry in entries {
                if entry.sequence != next_sequence {
                    return Err(IncarnationStreamError::EventSequence {
                        expected: next_sequence,
                        actual: entry.sequence,
                    });
                }
                let event = decode_event(&entry.payload)?;
                replay = replay.apply(event, entry.sequence)?;
                next_sequence = next_sequence
                    .checked_add(1)
                    .ok_or(IncarnationStreamError::StreamSequenceExhausted)?;
            }
        }
        Ok(ReplayedIncarnationState {
            allocator: replay.allocator,
            next_sequence,
            #[cfg(test)]
            generation: replay.generation,
            unmatched: replay.unmatched,
            #[cfg(test)]
            has_started: replay.has_started,
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
        if !replayed.unmatched.by_sequence.is_empty() {
            return Err(IncarnationStreamError::UnmatchedConnectionFates {
                count: replayed.unmatched.by_sequence.len(),
            });
        }
        let header = replayed.allocator.as_restore();
        Ok(StartedIncarnationStream {
            store: self.store,
            maximum_references: self.maximum_references,
            header,
            next_sequence: replayed.next_sequence,
            generation: replayed
                .generation
                .unwrap_or_else(|| AllocationGeneration::new(0, header.server_incarnation)),
        })
    }
}

/// Exclusive startup owner while historical Opens are completed under their persisted bound.
#[derive(Debug)]
pub struct ConnectionFateRecovery {
    #[cfg(test)]
    store: Arc<dyn DurableStore>,
    #[cfg(test)]
    maximum_references: usize,
    #[cfg(test)]
    allocator: ConnectionIncarnationAllocator,
    #[cfg(test)]
    next_sequence: u64,
    unmatched: UnmatchedConnectionFates,
}

impl ConnectionFateRecovery {
    #[must_use]
    pub fn intents(&self) -> Vec<ConnectionFateIntent> {
        self.unmatched.by_sequence.values().cloned().collect()
    }

    #[cfg(test)]
    pub async fn complete(&mut self, open_sequence: u64) -> Result<(), IncarnationStreamError> {
        if !self.unmatched.by_sequence.contains_key(&open_sequence) {
            return Err(IncarnationStreamError::RecoveryCompleteForAbsentOpen { open_sequence });
        }
        let payload = encode_event(&IncarnationEvent::CompleteConnectionFate {
            open_event_sequence: open_sequence,
        })?;
        let complete_sequence = self.next_sequence;
        self.next_sequence = append_and_flush(&self.store, complete_sequence, payload).await?;
        self.unmatched.complete(complete_sequence, open_sequence)?;
        Ok(())
    }

    #[cfg(test)]
    pub async fn finish_startup(self) -> Result<IncarnationStartup, IncarnationStreamError> {
        if !self.unmatched.by_sequence.is_empty() {
            return Err(IncarnationStreamError::RecoveryIncomplete {
                count: self.unmatched.by_sequence.len(),
            });
        }
        let startup_sequence = self.next_sequence;
        let payload = encode_event(&IncarnationEvent::Startup)?;
        match prepare_server_incarnation_startup(self.allocator) {
            ServerIncarnationStartupDecision::Fsync(intent) => {
                let next_sequence =
                    append_and_flush(&self.store, startup_sequence, payload).await?;
                let header = intent.complete_after_fsync().as_restore();
                Ok(IncarnationStartup::Started(StartedIncarnationStream {
                    store: self.store,
                    maximum_references: self.maximum_references,
                    header,
                    next_sequence,
                    generation: AllocationGeneration::new(
                        startup_sequence,
                        header.server_incarnation,
                    ),
                }))
            }
            ServerIncarnationStartupDecision::Exhausted(exhausted) => {
                Ok(IncarnationStartup::Exhausted(exhausted.outcome()))
            }
        }
    }
}

/// Started allocator whose latest server-startup input is already durable.
#[derive(Debug)]
pub(in crate::server) struct StartedIncarnationStream {
    store: Arc<dyn DurableStore>,
    maximum_references: usize,
    header: ConnectionIncarnationAllocatorRestore,
    next_sequence: u64,
    generation: AllocationGeneration,
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
        self.generation
            .establish_bound(self.next_sequence, self.maximum_references)?;
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
                retain_active_allocations(
                    &mut self.generation,
                    referenced_incarnations,
                    Some((connection_incarnation, self.next_sequence)),
                );
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
                if must_append {
                    retain_active_allocations(&mut self.generation, referenced_incarnations, None);
                }
                self.next_sequence = next_sequence;
                Ok(IncarnationAllocation::Exhausted(outcome))
            }
        }
    }
}

#[derive(Debug)]
struct ReplayAccumulator {
    allocator: ConnectionIncarnationAllocator,
    generation: Option<AllocationGeneration>,
    unmatched: UnmatchedConnectionFates,
    has_started: bool,
}

impl ReplayAccumulator {
    fn apply(
        self,
        event: IncarnationEvent,
        stored_sequence: u64,
    ) -> Result<Self, IncarnationStreamError> {
        match event {
            IncarnationEvent::Startup => self.apply_startup(stored_sequence),
            IncarnationEvent::Allocate {
                declared_reference_bound,
                referenced_incarnations,
            } => self.apply_allocate(
                stored_sequence,
                declared_reference_bound,
                &referenced_incarnations,
            ),
            IncarnationEvent::OpenConnectionFate {
                connection_incarnation,
                class,
                declared_conversation_bound,
                conversations,
            } => self.apply_open(
                stored_sequence,
                connection_incarnation,
                class,
                declared_conversation_bound,
                conversations,
            ),
            IncarnationEvent::CompleteConnectionFate {
                open_event_sequence,
            } => self.apply_complete(stored_sequence, open_event_sequence),
        }
    }

    fn apply_startup(mut self, stored_sequence: u64) -> Result<Self, IncarnationStreamError> {
        self.allocator = match prepare_server_incarnation_startup(self.allocator) {
            ServerIncarnationStartupDecision::Fsync(intent) => intent.complete_after_fsync(),
            ServerIncarnationStartupDecision::Exhausted(_) => {
                return Err(IncarnationStreamError::StartupAfterServerExhaustion {
                    stored_sequence,
                });
            }
        };
        self.generation = Some(AllocationGeneration::new(
            stored_sequence,
            self.allocator.as_restore().server_incarnation,
        ));
        self.has_started = true;
        Ok(self)
    }

    fn apply_allocate(
        mut self,
        stored_sequence: u64,
        declared_reference_bound: usize,
        referenced_incarnations: &[ConnectionIncarnation],
    ) -> Result<Self, IncarnationStreamError> {
        if !self.has_started {
            return Err(IncarnationStreamError::AllocateBeforeStartup { stored_sequence });
        }
        let Some(generation) = self.generation.as_mut() else {
            return Err(IncarnationStreamError::AllocateBeforeStartup { stored_sequence });
        };
        generation.establish_bound(stored_sequence, declared_reference_bound)?;
        let references = DurableIncarnationReferences::try_new(
            referenced_incarnations,
            declared_reference_bound,
        )?;
        let (allocator, produced) =
            match allocate_connection_incarnation(self.allocator, references) {
                ConnectionIncarnationAllocationDecision::Allocated(allocation) => {
                    let produced = allocation.connection_incarnation();
                    (allocation.into_resulting(), Some(produced))
                }
                ConnectionIncarnationAllocationDecision::Exhausted(exhaustion) => {
                    if matches!(
                        &exhaustion,
                        ConnectionOrdinalExhaustion::AlreadyExhausted(_)
                    ) {
                        return Err(IncarnationStreamError::AllocateAfterOrdinalExhaustion {
                            stored_sequence,
                        });
                    }
                    (exhaustion.into_resulting(), None)
                }
            };
        self.allocator = allocator;
        retain_active_allocations(
            generation,
            referenced_incarnations,
            produced.map(|connection| (connection, stored_sequence)),
        );
        Ok(self)
    }

    fn apply_open(
        mut self,
        stored_sequence: u64,
        connection_incarnation: ConnectionIncarnation,
        class: ConnectionFateClass,
        declared_conversation_bound: usize,
        conversations: Vec<u64>,
    ) -> Result<Self, IncarnationStreamError> {
        let Some(generation) = self.generation.as_ref() else {
            return Err(IncarnationStreamError::UnknownConnectionAllocation {
                open_sequence: stored_sequence,
                connection_incarnation,
            });
        };
        self.unmatched.open(
            stored_sequence,
            generation,
            connection_incarnation,
            class,
            declared_conversation_bound,
            conversations,
        )?;
        Ok(self)
    }

    fn apply_complete(
        mut self,
        stored_sequence: u64,
        open_event_sequence: u64,
    ) -> Result<Self, IncarnationStreamError> {
        self.unmatched
            .complete(stored_sequence, open_event_sequence)?;
        Ok(self)
    }
}

#[derive(Debug)]
struct ReplayedIncarnationState {
    allocator: ConnectionIncarnationAllocator,
    next_sequence: u64,
    #[cfg(test)]
    generation: Option<AllocationGeneration>,
    unmatched: UnmatchedConnectionFates,
    #[cfg(test)]
    has_started: bool,
}

fn retain_active_allocations(
    generation: &mut AllocationGeneration,
    referenced_incarnations: &[ConnectionIncarnation],
    produced: Option<(ConnectionIncarnation, u64)>,
) {
    let prior = core::mem::take(&mut generation.active_allocations);
    generation.active_allocations = referenced_incarnations
        .iter()
        .filter_map(|incarnation| {
            prior
                .get(incarnation)
                .map(|sequence| (*incarnation, *sequence))
        })
        .collect();
    if let Some((incarnation, sequence)) = produced {
        generation.active_allocations.insert(incarnation, sequence);
    }
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
        IncarnationEvent::OpenConnectionFate {
            declared_conversation_bound,
            conversations,
            ..
        } => {
            validate_conversations(*declared_conversation_bound, conversations)?;
            conversations
                .len()
                .checked_mul(CONVERSATION_ID_LEN)
                .and_then(|ids| ids.checked_add(OPEN_CONNECTION_FATE_FIXED_LEN))
                .ok_or(IncarnationStreamError::EventLengthOverflow)?
        }
        IncarnationEvent::CompleteConnectionFate { .. } => COMPLETE_CONNECTION_FATE_LEN,
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
        IncarnationEvent::OpenConnectionFate { .. } => OPEN_CONNECTION_FATE_TAG,
        IncarnationEvent::CompleteConnectionFate { .. } => COMPLETE_CONNECTION_FATE_TAG,
    });
    encoded.extend_from_slice(&declared_body_length.to_be_bytes());
    match event {
        IncarnationEvent::Startup => {}
        IncarnationEvent::Allocate {
            declared_reference_bound,
            referenced_incarnations,
        } => {
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
        IncarnationEvent::OpenConnectionFate {
            connection_incarnation,
            class,
            declared_conversation_bound,
            conversations,
        } => {
            let declared_conversation_bound = u64::try_from(*declared_conversation_bound)
                .map_err(|_| IncarnationStreamError::EventLengthOverflow)?;
            let count = u64::try_from(conversations.len())
                .map_err(|_| IncarnationStreamError::EventLengthOverflow)?;
            encoded.extend_from_slice(&connection_incarnation.server_incarnation.to_be_bytes());
            encoded.extend_from_slice(&connection_incarnation.connection_ordinal.to_be_bytes());
            encoded.push(class.tag());
            encoded.extend_from_slice(&declared_conversation_bound.to_be_bytes());
            encoded.extend_from_slice(&count.to_be_bytes());
            for conversation_id in conversations {
                encoded.extend_from_slice(&conversation_id.to_be_bytes());
            }
        }
        IncarnationEvent::CompleteConnectionFate {
            open_event_sequence,
        } => {
            encoded.extend_from_slice(&open_event_sequence.to_be_bytes());
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
    if version != FROZEN_EVENT_SCHEMA_VERSION && version != EVENT_SCHEMA_VERSION {
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
        OPEN_CONNECTION_FATE_TAG | COMPLETE_CONNECTION_FATE_TAG
            if version == FROZEN_EVENT_SCHEMA_VERSION =>
        {
            Err(IncarnationStreamError::EventKindSchemaVersion { version, kind })
        }
        OPEN_CONNECTION_FATE_TAG => decode_open_connection_fate_event(body),
        COMPLETE_CONNECTION_FATE_TAG => decode_complete_connection_fate_event(body),
        unknown => Err(IncarnationStreamError::EventKind(unknown)),
    }
}

fn validate_conversations(
    declared_conversation_bound: usize,
    conversations: &[u64],
) -> Result<(), IncarnationStreamError> {
    if conversations.len() > declared_conversation_bound {
        return Err(IncarnationStreamError::ConversationCountExceedsBound {
            actual: conversations.len(),
            maximum: declared_conversation_bound,
        });
    }
    for (index, pair) in conversations.windows(2).enumerate() {
        if pair[0] >= pair[1] {
            return Err(IncarnationStreamError::ConversationsNotStrictlyIncreasing {
                index: index
                    .checked_add(1)
                    .ok_or(IncarnationStreamError::EventLengthOverflow)?,
            });
        }
    }
    Ok(())
}

fn decode_open_connection_fate_event(
    body: &[u8],
) -> Result<IncarnationEvent, IncarnationStreamError> {
    if body.len() < OPEN_CONNECTION_FATE_FIXED_LEN {
        return Err(IncarnationStreamError::EventTruncated {
            required: EVENT_HEADER_LEN + OPEN_CONNECTION_FATE_FIXED_LEN,
            actual: EVENT_HEADER_LEN + body.len(),
        });
    }
    let connection_incarnation = ConnectionIncarnation::new(take_u64(body, 0)?, take_u64(body, 8)?);
    let class = ConnectionFateClass::from_tag(take_u8(body, CONNECTION_INCARNATION_LEN)?)?;
    let bound_offset = CONNECTION_INCARNATION_LEN + CONNECTION_FATE_CLASS_LEN;
    let bound = take_u64(body, bound_offset)?;
    let declared_conversation_bound = usize::try_from(bound)
        .map_err(|_| IncarnationStreamError::ConversationBoundPlatformOverflow { bound })?;
    let count_offset = bound_offset + CONVERSATION_BOUND_LEN;
    let count = take_u64(body, count_offset)?;
    let count_usize = usize::try_from(count)
        .map_err(|_| IncarnationStreamError::ConversationCountPlatformOverflow { count })?;
    let expected = count_usize
        .checked_mul(CONVERSATION_ID_LEN)
        .and_then(|ids| ids.checked_add(OPEN_CONNECTION_FATE_FIXED_LEN))
        .ok_or(IncarnationStreamError::EventLengthOverflow)?;
    if expected != body.len() {
        return Err(IncarnationStreamError::OpenConnectionFateBodyLength {
            count,
            expected,
            actual: body.len(),
        });
    }
    let mut conversations = Vec::with_capacity(count_usize);
    let mut offset = OPEN_CONNECTION_FATE_FIXED_LEN;
    for _ in 0..count_usize {
        conversations.push(take_u64(body, offset)?);
        offset = offset
            .checked_add(CONVERSATION_ID_LEN)
            .ok_or(IncarnationStreamError::EventLengthOverflow)?;
    }
    validate_conversations(declared_conversation_bound, &conversations)?;
    Ok(IncarnationEvent::OpenConnectionFate {
        connection_incarnation,
        class,
        declared_conversation_bound,
        conversations,
    })
}

fn decode_complete_connection_fate_event(
    body: &[u8],
) -> Result<IncarnationEvent, IncarnationStreamError> {
    if body.len() != COMPLETE_CONNECTION_FATE_LEN {
        return Err(IncarnationStreamError::CompleteConnectionFateBodyLength {
            actual: body.len(),
        });
    }
    Ok(IncarnationEvent::CompleteConnectionFate {
        open_event_sequence: take_u64(body, 0)?,
    })
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
pub(in crate::server) fn encode_frozen_v1_startup_event_fixture()
-> Result<Vec<u8>, IncarnationStreamError> {
    let mut encoded = encode_startup_event_fixture()?;
    encoded[4] = FROZEN_EVENT_SCHEMA_VERSION;
    Ok(encoded)
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

#[cfg(test)]
pub(in crate::server) fn encode_open_connection_fate_event_fixture(
    connection_incarnation: ConnectionIncarnation,
    class: ConnectionFateClass,
    declared_conversation_bound: usize,
    conversations: &[u64],
) -> Result<Vec<u8>, IncarnationStreamError> {
    encode_event(&IncarnationEvent::OpenConnectionFate {
        connection_incarnation,
        class,
        declared_conversation_bound,
        conversations: conversations.to_vec(),
    })
}

#[cfg(test)]
pub(in crate::server) fn encode_complete_connection_fate_event_fixture(
    open_event_sequence: u64,
) -> Result<Vec<u8>, IncarnationStreamError> {
    encode_event(&IncarnationEvent::CompleteConnectionFate {
        open_event_sequence,
    })
}
