//! Monotonic connection-incarnation allocation.
//!
//! Frozen `PARTICIPANT-CONTRACT.md` lines 484-504 require a checked, fsynced
//! startup increment of `server_incarnation`, followed by checked connection
//! ordinals which never wrap, rebase, or reuse a value. Allocation checks every
//! supplied live or durable reference before publication. The supplied
//! reference collection is explicitly bounded so collision retry is finite.
//!
//! The transition results own their resulting header. A server persists that
//! header in the same transaction which publishes the allocated incarnation;
//! replay from the same raw pre-state and reference set is deterministic.

use crate::{outcome::ConnectionIncarnationExhausted, wire::ConnectionIncarnation};

/// Raw durable connection-incarnation allocator header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConnectionIncarnationAllocatorRestore {
    /// Server incarnation persisted by the most recent startup fsync.
    pub server_incarnation: u64,
    /// Last ordinal allocated or rejected because a durable reference owned it.
    pub last_examined_connection_ordinal: Option<u64>,
    /// Fixed header bit which permanently closes this incarnation's ordinal space.
    pub connection_ordinal_exhausted: bool,
}

/// Invalid durable connection-incarnation allocator header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionIncarnationAllocatorRestoreError {
    /// The exhaustion bit and terminal examined ordinal disagree.
    OrdinalExhaustionMismatch {
        /// Restored last examined ordinal.
        last_examined_connection_ordinal: Option<u64>,
        /// Restored fixed exhaustion bit.
        connection_ordinal_exhausted: bool,
    },
}

/// Validated monotonic connection-incarnation allocator state.
///
/// This state is intentionally not `Copy` or `Clone`: each transition consumes
/// its pre-state and returns the one resulting state which must be committed.
#[derive(Debug, PartialEq, Eq)]
pub struct ConnectionIncarnationAllocator {
    server_incarnation: u64,
    last_examined_connection_ordinal: Option<u64>,
    connection_ordinal_exhausted: bool,
}

impl ConnectionIncarnationAllocator {
    /// Restores the durable fixed header after validating the terminal bit.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectionIncarnationAllocatorRestoreError`] unless ordinal
    /// `u64::MAX` and the exhaustion bit are either both present or both absent.
    pub const fn try_restore(
        restored: ConnectionIncarnationAllocatorRestore,
    ) -> Result<Self, ConnectionIncarnationAllocatorRestoreError> {
        let examined_max = matches!(restored.last_examined_connection_ordinal, Some(u64::MAX));
        if examined_max != restored.connection_ordinal_exhausted {
            return Err(
                ConnectionIncarnationAllocatorRestoreError::OrdinalExhaustionMismatch {
                    last_examined_connection_ordinal: restored.last_examined_connection_ordinal,
                    connection_ordinal_exhausted: restored.connection_ordinal_exhausted,
                },
            );
        }

        Ok(Self {
            server_incarnation: restored.server_incarnation,
            last_examined_connection_ordinal: restored.last_examined_connection_ordinal,
            connection_ordinal_exhausted: restored.connection_ordinal_exhausted,
        })
    }

    /// Returns the current durable server incarnation.
    #[must_use]
    pub const fn server_incarnation(&self) -> u64 {
        self.server_incarnation
    }

    /// Returns the last allocated or collision-rejected connection ordinal.
    #[must_use]
    pub const fn last_examined_connection_ordinal(&self) -> Option<u64> {
        self.last_examined_connection_ordinal
    }

    /// Returns whether this server incarnation can never mint another ordinal.
    #[must_use]
    pub const fn connection_ordinal_exhausted(&self) -> bool {
        self.connection_ordinal_exhausted
    }

    /// Projects the exact fixed header which a binding must persist atomically.
    #[must_use]
    pub const fn as_restore(&self) -> ConnectionIncarnationAllocatorRestore {
        ConnectionIncarnationAllocatorRestore {
            server_incarnation: self.server_incarnation,
            last_examined_connection_ordinal: self.last_examined_connection_ordinal,
            connection_ordinal_exhausted: self.connection_ordinal_exhausted,
        }
    }
}

/// Startup decision for the checked server-incarnation increment.
#[derive(Debug, PartialEq, Eq)]
pub enum ServerIncarnationStartupDecision {
    /// Persist and fsync the checked increment before participant mode starts.
    Fsync(ServerIncarnationFsyncIntent),
    /// The persisted server counter is already terminal and remains unchanged.
    Exhausted(ServerIncarnationExhaustion),
}

/// Checked startup write which must be fsynced before participant mode starts.
#[derive(Debug, PartialEq, Eq)]
pub struct ServerIncarnationFsyncIntent {
    prior_server_incarnation: u64,
    resulting: ConnectionIncarnationAllocator,
}

impl ServerIncarnationFsyncIntent {
    /// Returns the durable counter value from before this startup.
    #[must_use]
    pub const fn prior_server_incarnation(&self) -> u64 {
        self.prior_server_incarnation
    }

    /// Returns the checked server incarnation to persist and fsync.
    #[must_use]
    pub const fn server_incarnation(&self) -> u64 {
        self.resulting.server_incarnation
    }

    /// Returns the exact fresh-ordinal header to persist and fsync.
    #[must_use]
    pub const fn header_to_fsync(&self) -> ConnectionIncarnationAllocatorRestore {
        self.resulting.as_restore()
    }

    /// Releases the current allocator after the server has persisted and fsynced
    /// [`Self::header_to_fsync`] in its startup transaction.
    #[must_use]
    pub const fn complete_after_fsync(self) -> ConnectionIncarnationAllocator {
        self.resulting
    }
}

/// State-preserving terminal server-incarnation refusal.
#[derive(Debug, PartialEq, Eq)]
pub struct ServerIncarnationExhaustion {
    unchanged: ConnectionIncarnationAllocator,
}

impl ServerIncarnationExhaustion {
    /// Returns the exact R-D1 exhaustion payload.
    #[must_use]
    pub const fn outcome(&self) -> ConnectionIncarnationExhausted {
        ConnectionIncarnationExhausted::ServerIncarnation
    }

    /// Returns the unchanged terminal durable state.
    #[must_use]
    pub const fn into_unchanged(self) -> ConnectionIncarnationAllocator {
        self.unchanged
    }
}

/// Produces the checked startup fsync intent or exact server exhaustion.
///
/// The successful intent resets the ordinal namespace only because it owns a
/// strictly newer server incarnation. No startup path wraps or reuses a server
/// value.
#[must_use]
pub const fn prepare_server_incarnation_startup(
    persisted: ConnectionIncarnationAllocator,
) -> ServerIncarnationStartupDecision {
    let Some(server_incarnation) = persisted.server_incarnation.checked_add(1) else {
        return ServerIncarnationStartupDecision::Exhausted(ServerIncarnationExhaustion {
            unchanged: persisted,
        });
    };

    ServerIncarnationStartupDecision::Fsync(ServerIncarnationFsyncIntent {
        prior_server_incarnation: persisted.server_incarnation,
        resulting: ConnectionIncarnationAllocator {
            server_incarnation,
            last_examined_connection_ordinal: None,
            connection_ordinal_exhausted: false,
        },
    })
}

/// Error constructing a bounded complete durable-reference collection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DurableIncarnationReferencesError {
    /// The supplied complete reference collection exceeds its configured bound.
    ReferenceCountExceedsBound {
        /// Number of supplied live and durable references.
        actual: usize,
        /// Maximum reference count proved by server storage configuration.
        maximum: usize,
    },
}

/// Complete bounded live and durable connection-incarnation references.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DurableIncarnationReferences<'a> {
    values: &'a [ConnectionIncarnation],
}

impl<'a> DurableIncarnationReferences<'a> {
    /// Validates the complete reference collection against its storage bound.
    ///
    /// Duplicates are legal because one incarnation can be named by a binding,
    /// receipt, work item, and recovery row simultaneously.
    ///
    /// # Errors
    ///
    /// Returns [`DurableIncarnationReferencesError`] if `values` exceeds the
    /// configured maximum reference count.
    pub const fn try_new(
        values: &'a [ConnectionIncarnation],
        maximum: usize,
    ) -> Result<Self, DurableIncarnationReferencesError> {
        if values.len() > maximum {
            return Err(
                DurableIncarnationReferencesError::ReferenceCountExceedsBound {
                    actual: values.len(),
                    maximum,
                },
            );
        }
        Ok(Self { values })
    }

    /// Returns the supplied complete reference collection.
    #[must_use]
    pub const fn as_slice(self) -> &'a [ConnectionIncarnation] {
        self.values
    }

    const fn collides(self, candidate: ConnectionIncarnation) -> bool {
        let mut index = 0;
        while index < self.values.len() {
            let reference = self.values[index];
            if reference.server_incarnation == candidate.server_incarnation
                && reference.connection_ordinal == candidate.connection_ordinal
            {
                return true;
            }
            index += 1;
        }
        false
    }
}

/// Connection-ordinal allocation decision.
#[derive(Debug, PartialEq, Eq)]
pub enum ConnectionIncarnationAllocationDecision {
    /// One unique incarnation is ready for atomic header persistence/publication.
    Allocated(ConnectionIncarnationAllocation),
    /// No ordinal remains for this server incarnation.
    Exhausted(ConnectionOrdinalExhaustion),
}

/// Successful connection-incarnation allocation and resulting durable state.
#[derive(Debug, PartialEq, Eq)]
pub struct ConnectionIncarnationAllocation {
    connection_incarnation: ConnectionIncarnation,
    skipped_collisions: usize,
    resulting: ConnectionIncarnationAllocator,
}

impl ConnectionIncarnationAllocation {
    /// Returns the unique pair to publish only with the resulting header commit.
    #[must_use]
    pub const fn connection_incarnation(&self) -> ConnectionIncarnation {
        self.connection_incarnation
    }

    /// Returns how many referenced candidates were skipped before publication.
    #[must_use]
    pub const fn skipped_collisions(&self) -> usize {
        self.skipped_collisions
    }

    /// Returns the exact resulting fixed header.
    #[must_use]
    pub const fn resulting_header(&self) -> ConnectionIncarnationAllocatorRestore {
        self.resulting.as_restore()
    }

    /// Consumes the commit and returns the resulting allocator state.
    #[must_use]
    pub const fn into_resulting(self) -> ConnectionIncarnationAllocator {
        self.resulting
    }
}

/// Exact ordinal-exhaustion transition.
#[derive(Debug, PartialEq, Eq)]
pub enum ConnectionOrdinalExhaustion {
    /// A referenced `u64::MAX` candidate atomically sets the terminal bit.
    MarkExhausted(ConnectionOrdinalExhaustionCommit),
    /// The terminal bit was already durable, so the refusal changes no state.
    AlreadyExhausted(ConnectionOrdinalExhaustionReplay),
}

impl ConnectionOrdinalExhaustion {
    /// Returns the exact R-D1 ordinal exhaustion payload.
    #[must_use]
    pub const fn outcome(&self) -> ConnectionIncarnationExhausted {
        let attempted_server_incarnation = match self {
            Self::MarkExhausted(commit) => commit.resulting.server_incarnation,
            Self::AlreadyExhausted(replay) => replay.unchanged.server_incarnation,
        };
        ConnectionIncarnationExhausted::ConnectionOrdinal {
            attempted_server_incarnation,
        }
    }

    /// Returns the exact resulting fixed header for commit or unchanged replay.
    #[must_use]
    pub const fn resulting_header(&self) -> ConnectionIncarnationAllocatorRestore {
        match self {
            Self::MarkExhausted(commit) => commit.resulting.as_restore(),
            Self::AlreadyExhausted(replay) => replay.unchanged.as_restore(),
        }
    }

    /// Consumes the transition and returns its resulting allocator state.
    #[must_use]
    pub const fn into_resulting(self) -> ConnectionIncarnationAllocator {
        match self {
            Self::MarkExhausted(commit) => commit.resulting,
            Self::AlreadyExhausted(replay) => replay.unchanged,
        }
    }
}

/// Atomic fixed-header update after a referenced terminal collision.
#[derive(Debug, PartialEq, Eq)]
pub struct ConnectionOrdinalExhaustionCommit {
    skipped_collisions: usize,
    resulting: ConnectionIncarnationAllocator,
}

impl ConnectionOrdinalExhaustionCommit {
    /// Returns how many referenced candidates, including MAX, were skipped.
    #[must_use]
    pub const fn skipped_collisions(&self) -> usize {
        self.skipped_collisions
    }

    /// Returns the exact fixed header which atomically sets exhaustion.
    #[must_use]
    pub const fn resulting_header(&self) -> ConnectionIncarnationAllocatorRestore {
        self.resulting.as_restore()
    }
}

/// Idempotent refusal from an already terminal connection-ordinal header.
#[derive(Debug, PartialEq, Eq)]
pub struct ConnectionOrdinalExhaustionReplay {
    unchanged: ConnectionIncarnationAllocator,
}

impl ConnectionOrdinalExhaustionReplay {
    /// Returns the exact unchanged terminal header.
    #[must_use]
    pub const fn unchanged_header(&self) -> ConnectionIncarnationAllocatorRestore {
        self.unchanged.as_restore()
    }
}

/// Allocates one checked connection ordinal, skipping every exact collision.
///
/// A successful MAX allocation and a referenced MAX collision both atomically
/// set `connection_ordinal_exhausted`. A subsequent call has no candidate and
/// returns the stable [`ConnectionOrdinalExhaustion::AlreadyExhausted`] arm.
#[must_use]
pub fn allocate_connection_incarnation(
    allocator: ConnectionIncarnationAllocator,
    references: DurableIncarnationReferences<'_>,
) -> ConnectionIncarnationAllocationDecision {
    if allocator.connection_ordinal_exhausted {
        return ConnectionIncarnationAllocationDecision::Exhausted(
            ConnectionOrdinalExhaustion::AlreadyExhausted(ConnectionOrdinalExhaustionReplay {
                unchanged: allocator,
            }),
        );
    }

    let next_candidate = allocator
        .last_examined_connection_ordinal
        .map_or(Some(0), |last| last.checked_add(1));
    let Some(mut candidate_ordinal) = next_candidate else {
        return ConnectionIncarnationAllocationDecision::Exhausted(
            ConnectionOrdinalExhaustion::AlreadyExhausted(ConnectionOrdinalExhaustionReplay {
                unchanged: allocator,
            }),
        );
    };
    let mut skipped_collisions = 0;

    loop {
        let candidate = ConnectionIncarnation::new(allocator.server_incarnation, candidate_ordinal);
        if !references.collides(candidate) {
            let connection_ordinal_exhausted = candidate_ordinal == u64::MAX;
            return ConnectionIncarnationAllocationDecision::Allocated(
                ConnectionIncarnationAllocation {
                    connection_incarnation: candidate,
                    skipped_collisions,
                    resulting: ConnectionIncarnationAllocator {
                        server_incarnation: allocator.server_incarnation,
                        last_examined_connection_ordinal: Some(candidate_ordinal),
                        connection_ordinal_exhausted,
                    },
                },
            );
        }

        skipped_collisions += 1;
        if candidate_ordinal == u64::MAX {
            return ConnectionIncarnationAllocationDecision::Exhausted(
                ConnectionOrdinalExhaustion::MarkExhausted(ConnectionOrdinalExhaustionCommit {
                    skipped_collisions,
                    resulting: ConnectionIncarnationAllocator {
                        server_incarnation: allocator.server_incarnation,
                        last_examined_connection_ordinal: Some(u64::MAX),
                        connection_ordinal_exhausted: true,
                    },
                }),
            );
        }
        candidate_ordinal += 1;
    }
}
