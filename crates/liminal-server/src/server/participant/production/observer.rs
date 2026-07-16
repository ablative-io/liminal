//! Durable row log for the server-wide observer-recovery aggregate.
//!
//! Rows persist observer-progress registrations, progress advances, and
//! installed arm plans. Restore folds every row back through the aggregate's
//! own A4 transactions, so validation lives in the protocol crate — a row the
//! aggregate would refuse fails restore loudly.

use std::sync::Arc;

use liminal::durability::DurableStore;
use liminal_protocol::lifecycle::{
    ObserverProgressAdvanceDecision, ObserverProgressTrackDecision, ObserverRecoveryAggregate,
};
use liminal_protocol::wire::{ConversationId, DeliverySeq, ObserverEpoch};
use serde::{Deserialize, Serialize};

use super::log::OperationLogError;

/// Stream key of the server-wide observer-recovery row log.
const OBSERVER_STREAM_KEY: &str = "liminal:participant-observer-recovery";
/// Durable page size used during restore reads.
const READ_BATCH_SIZE: usize = 64;
/// Stored-row schema version.
const SCHEMA_VERSION: u8 = 1;

/// One durable observer-recovery row.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case", tag = "row")]
pub(super) enum ObserverRow {
    /// A conversation's observer progress became tracked.
    Track {
        /// Newly tracked conversation.
        conversation_id: ConversationId,
        /// Initial durable observer progress.
        observer_progress: DeliverySeq,
    },
    /// A tracked conversation's observer progress advanced.
    Advance {
        /// Advancing conversation.
        conversation_id: ConversationId,
        /// New strictly greater progress.
        observer_progress: DeliverySeq,
    },
    /// One complete equal-epoch arm plan was installed atomically.
    Arms {
        /// Every arm of the plan, in conversation order.
        arms: Vec<(ConversationId, ObserverEpoch)>,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct StoredRow {
    schema_version: u8,
    row: ObserverRow,
}

/// Restored aggregate together with the next durable row sequence.
#[derive(Debug)]
pub(super) struct RestoredObserver {
    /// Aggregate rebuilt through its own transactions.
    pub(super) aggregate: ObserverRecoveryAggregate,
    /// Optimistic head for the next append.
    pub(super) next_sequence: u64,
}

/// Append-only handle over the observer row stream.
#[derive(Debug)]
pub(super) struct ObserverLog {
    store: Arc<dyn DurableStore>,
}

impl ObserverLog {
    /// Creates a stateless handle over the shared store.
    pub(super) fn new(store: Arc<dyn DurableStore>) -> Self {
        Self { store }
    }

    /// Appends one row at the optimistic head, then flushes.
    pub(super) async fn append(
        &self,
        row: &ObserverRow,
        expected_sequence: u64,
    ) -> Result<(), OperationLogError> {
        let payload = serde_json::to_vec(&StoredRow {
            schema_version: SCHEMA_VERSION,
            row: row.clone(),
        })?;
        let assigned = self
            .store
            .append(OBSERVER_STREAM_KEY, payload, expected_sequence)
            .await?;
        if assigned != expected_sequence {
            return Err(OperationLogError::AssignedSequence {
                expected: expected_sequence,
                actual: assigned,
            });
        }
        self.store.flush().await?;
        Ok(())
    }

    /// Rebuilds the aggregate by folding every durable row through the
    /// aggregate's own transactions.
    pub(super) async fn restore(&self) -> Result<RestoredObserver, OperationLogError> {
        let mut aggregate = ObserverRecoveryAggregate::new();
        let mut sequence = 0_u64;
        loop {
            let entries = self
                .store
                .read_from(OBSERVER_STREAM_KEY, sequence, READ_BATCH_SIZE)
                .await?;
            if entries.is_empty() {
                break;
            }
            let count = entries.len();
            for entry in entries {
                if entry.sequence != sequence {
                    return Err(OperationLogError::Sequence {
                        expected: sequence,
                        actual: entry.sequence,
                    });
                }
                let stored: StoredRow = serde_json::from_slice(&entry.payload)?;
                if stored.schema_version != SCHEMA_VERSION {
                    return Err(OperationLogError::SchemaVersion(stored.schema_version));
                }
                aggregate = fold_row(aggregate, stored.row, entry.sequence)?;
                sequence = sequence.checked_add(1).ok_or(OperationLogError::Sequence {
                    expected: u64::MAX,
                    actual: entry.sequence,
                })?;
            }
            if count < READ_BATCH_SIZE {
                break;
            }
        }
        Ok(RestoredObserver {
            aggregate,
            next_sequence: sequence,
        })
    }
}

/// Applies one durable row through the aggregate's own barriered mutations.
fn fold_row(
    aggregate: ObserverRecoveryAggregate,
    row: ObserverRow,
    sequence: u64,
) -> Result<ObserverRecoveryAggregate, OperationLogError> {
    match row {
        ObserverRow::Track {
            conversation_id,
            observer_progress,
        } => match aggregate.decide_track(conversation_id, observer_progress) {
            ObserverProgressTrackDecision::Commit(transaction) => Ok(transaction.commit()),
            ObserverProgressTrackDecision::Refuse { .. } => Err(refused(sequence)),
        },
        ObserverRow::Advance {
            conversation_id,
            observer_progress,
        } => match aggregate.decide_progress_advance(conversation_id, observer_progress) {
            ObserverProgressAdvanceDecision::Commit(transaction) => {
                let (aggregate, _fired) = transaction.commit();
                Ok(aggregate)
            }
            ObserverProgressAdvanceDecision::Refuse { .. } => Err(refused(sequence)),
        },
        ObserverRow::Arms { arms } => {
            // Arm plans install only through a full recovery decision; restore
            // rebuilds them via the validated row-level constructor.
            let progress = aggregate.progress_rows();
            let mut armed = aggregate.armed_rows();
            armed.extend(arms);
            armed.sort_unstable();
            armed.dedup();
            ObserverRecoveryAggregate::restore(&progress, &armed).map_err(|_| refused(sequence))
        }
    }
}

/// A durable row the aggregate itself refuses is a corrupt stream.
const fn refused(sequence: u64) -> OperationLogError {
    OperationLogError::CorruptRow { sequence }
}
