//! The single production participant semantic handler.
//!
//! One handler per server. Each conversation has exactly one live in-memory
//! authority owner, rebuilt from its durable transition-input log on first
//! touch (and after any failed operation, which discards the owner so the
//! next touch cold-replays durable reality). A short registry lock selects
//! the conversation cell; a per-conversation lock covers replay and each
//! operation. Everything is event-driven: cells are created on request
//! arrival, discarded on error, and evicted entirely when the touched
//! conversation has no durable log — no timer, sweep, or polling loop
//! exists, and refused probes of unknown conversation ids leave neither
//! durable nor in-memory residue.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard};

use liminal::durability::DurableStore;
use liminal::durability::bridge::block_on;
use liminal_protocol::lifecycle::{CapacityCounter, ObserverRecoveryAggregate};
use liminal_protocol::wire::ConversationId;

use crate::config::types::ParticipantConfig;
use crate::server::participant::{
    ObserverPublicationTarget, ParticipantConnectionContext, ParticipantConnectionConversations,
    ParticipantSemanticError, ParticipantServiceFatal,
};

use super::barrier::{OperationFacts, ReceiptCapacityLimits};
use super::capacity::ServerCapacity;
use super::facts;
use super::log::{OperationLog, OperationLogError, StoredOperation};
use super::outbox::ConversationOutboxLimits;
use super::outbox_log::{OutboxLog, OutboxLogError};
use super::outbox_replay::RestoreError;
use super::registry::ConversationRegistry;
use super::state::{ConversationAuthority, DurableAppend, StateError};

#[derive(Debug)]
pub(super) struct ObserverArmTarget {
    pub(super) refused_epoch: u64,
    pub(super) connection_incarnation: liminal_protocol::wire::ConnectionIncarnation,
    pub(super) target: ObserverPublicationTarget,
}

/// One exclusively serialized observer owner: durable protocol aggregate/head
/// plus volatile weak live targets for its installed arms. Restoring durable
/// arms intentionally starts with an empty target map because no socket
/// survives a process restart.
#[derive(Debug)]
pub(super) struct ObserverOwner {
    pub(super) aggregate: ObserverRecoveryAggregate,
    pub(super) head: u64,
    pub(super) arm_targets: BTreeMap<ConversationId, ObserverArmTarget>,
}

/// Production semantic handler backed by the shared durable store.
///
/// Constructed exactly once per server by the connection-services layer when
/// the deployment's `[participant]` configuration is present.
#[derive(Debug)]
pub struct ProductionParticipantHandler {
    pub(super) store: Arc<dyn DurableStore>,
    pub(super) config: ParticipantConfig,
    outbox_limits: ConversationOutboxLimits,
    conversations: Mutex<HashMap<ConversationId, Arc<Mutex<Option<ConversationAuthority>>>>>,
    /// First post-Open fatal; once set, every semantic/publication entry refuses.
    service_fatal: Mutex<Option<ParticipantServiceFatal>>,
    /// Server-wide observer-recovery aggregate paired with its durable row
    /// head (`None` until first restored).
    pub(super) observer: Mutex<Option<ObserverOwner>>,
    /// Server-scope stage-8 occupancy ledger (identity slots, live receipts,
    /// provenance fingerprints), restored from every durable conversation at
    /// construction and kept exact by commit reservations and replay folds.
    pub(super) capacity: ServerCapacity,
    /// Durable registry of created conversations: one row appended before
    /// each conversation's genesis append, read at startup to enumerate
    /// every durable conversation for the capacity restore.
    registry: ConversationRegistry,
}

impl ProductionParticipantHandler {
    /// Creates the handler over the server's shared durable store, replaying
    /// every durable conversation so the server-scope capacity ledger is
    /// exact against durable truth from the first request (a restart must
    /// not forget reserved identity slots or in-window receipts).
    ///
    /// # Errors
    ///
    /// Returns [`ParticipantSemanticError`] when the durable store cannot be
    /// scanned or a conversation log fails replay — the server refuses to
    /// start over state it cannot account for.
    pub fn new(
        store: Arc<dyn DurableStore>,
        config: ParticipantConfig,
    ) -> Result<Self, ParticipantSemanticError> {
        let outbox_limits = ConversationOutboxLimits::try_new(
            config.max_retained_record_rows,
            config.identity_slots,
        )
        .map_err(|error| ParticipantSemanticError::Internal {
            message: format!("participant outbox limit configuration failed: {error}"),
        })?;
        let registry = ConversationRegistry::new(Arc::clone(&store));
        let handler = Self {
            store,
            config,
            outbox_limits,
            conversations: Mutex::new(HashMap::new()),
            service_fatal: Mutex::new(None),
            observer: Mutex::new(None),
            capacity: ServerCapacity::default(),
            registry,
        };
        handler.restore_all_conversations()?;
        Ok(handler)
    }

    pub(super) fn current_service_fatal(
        &self,
    ) -> Result<Option<ParticipantServiceFatal>, ParticipantSemanticError> {
        let fatal = self
            .service_fatal
            .lock()
            .map_err(|_| ParticipantSemanticError::Internal {
                message: "participant service fatal latch is poisoned".to_owned(),
            })?
            .clone();
        Ok(fatal)
    }

    pub(super) fn ensure_service_live(&self) -> Result<(), ParticipantSemanticError> {
        if let Some(fatal) = self.current_service_fatal()? {
            return Err(ParticipantSemanticError::ServiceFatal(fatal));
        }
        Ok(())
    }

    pub(super) fn latch_connection_fate_fatal(
        &self,
        open_sequence: u64,
        conversation_id: ConversationId,
    ) -> Result<ParticipantServiceFatal, ParticipantSemanticError> {
        let mut fatal =
            self.service_fatal
                .lock()
                .map_err(|_| ParticipantSemanticError::Internal {
                    message: "participant service fatal latch is poisoned".to_owned(),
                })?;
        let selected = fatal
            .get_or_insert_with(|| ParticipantServiceFatal::ConnectionFateIntentIncomplete {
                open_sequence,
                conversation_id,
            })
            .clone();
        drop(fatal);
        Ok(selected)
    }

    /// Startup restore: enumerates every registered conversation and replays
    /// it, folding each conversation's server-scope contribution into the
    /// capacity ledger.
    ///
    /// A registry row whose conversation never got its genesis append (the
    /// crash window between the two ordered appends) replays empty and is
    /// evicted exactly like a refused probe.
    fn restore_all_conversations(&self) -> Result<(), ParticipantSemanticError> {
        let conversation_ids = self.registry.restore().map_err(|error| log_error(&error))?;
        for conversation_id in conversation_ids {
            let cell = self.cell(conversation_id)?;
            let mut owner = cell
                .lock()
                .map_err(|_| ParticipantSemanticError::Internal {
                    message: format!(
                        "participant conversation {conversation_id} owner lock is poisoned"
                    ),
                })?;
            if owner.is_none() {
                let log = OperationLog::new(Arc::clone(&self.store), conversation_id);
                let replayed = self.replay_and_repair(conversation_id, &log)?;
                let durably_empty = replayed.next_log_sequence == 0;
                if durably_empty {
                    drop(owner);
                    self.evict_uncommitted(conversation_id, &cell)?;
                    continue;
                }
                *owner = Some(replayed);
            }
            drop(owner);
        }
        Ok(())
    }

    pub(super) fn registered_conversation_ids(
        &self,
    ) -> Result<Vec<ConversationId>, ParticipantSemanticError> {
        self.registry.restore().map_err(|error| log_error(&error))
    }

    pub(super) fn cell(
        &self,
        conversation_id: ConversationId,
    ) -> Result<Arc<Mutex<Option<ConversationAuthority>>>, ParticipantSemanticError> {
        let mut conversations =
            self.conversations
                .lock()
                .map_err(|_| ParticipantSemanticError::Internal {
                    message: "participant conversation registry lock is poisoned".to_owned(),
                })?;
        let cell = Arc::clone(
            conversations
                .entry(conversation_id)
                .or_insert_with(|| Arc::new(Mutex::new(None))),
        );
        drop(conversations);
        Ok(cell)
    }

    /// Runs one operation over the exclusively owned conversation authority.
    ///
    /// On any [`StateError`] the in-memory owner is discarded — durable state
    /// is untouched by failed operations, so the next touch cold-replays
    /// exact durable reality. A conversation whose durable log is still empty
    /// after the operation (a refused or failed probe of a never-committed
    /// conversation id) leaves no residue: its registry cell is evicted, so
    /// wire probes grow neither durable nor in-memory state.
    pub(super) fn with_conversation<T>(
        &self,
        conversation_id: ConversationId,
        operation: impl FnOnce(&mut ConversationAuthority, &dyn DurableAppend) -> Result<T, StateError>,
    ) -> Result<T, ParticipantSemanticError> {
        self.with_conversation_reconciliation(conversation_id, true, operation)
    }

    /// Runs a fate-source append and reconciles its exact Unit 2 projection before
    /// retaining the transitioned live owner. Died/Detached joined the exhaustive
    /// v3 replay projection pass with W1b, so this boundary must use the same
    /// post-append repair as semantic sources to keep live and cold owners equal.
    pub(super) fn with_conversation_fate_source<T>(
        &self,
        conversation_id: ConversationId,
        operation: impl FnOnce(&mut ConversationAuthority, &dyn DurableAppend) -> Result<T, StateError>,
    ) -> Result<T, ParticipantSemanticError> {
        self.with_conversation_reconciliation(conversation_id, true, operation)
    }

    fn with_conversation_reconciliation<T>(
        &self,
        conversation_id: ConversationId,
        reconcile_appended_source: bool,
        operation: impl FnOnce(&mut ConversationAuthority, &dyn DurableAppend) -> Result<T, StateError>,
    ) -> Result<T, ParticipantSemanticError> {
        let cell = self.cell(conversation_id)?;
        let mut owner: MutexGuard<'_, Option<ConversationAuthority>> =
            cell.lock()
                .map_err(|_| ParticipantSemanticError::Internal {
                    message: format!(
                        "participant conversation {conversation_id} owner lock is poisoned"
                    ),
                })?;
        let log = OperationLog::new(Arc::clone(&self.store), conversation_id);
        if owner.is_none() {
            let replayed = self.replay_and_repair(conversation_id, &log)?;
            *owner = Some(replayed);
        }
        let Some(authority) = owner.as_mut() else {
            return Err(ParticipantSemanticError::Internal {
                message: format!("participant conversation {conversation_id} owner is absent"),
            });
        };
        let appender = LogAppender {
            log: &log,
            registry: &self.registry,
            conversation_id,
        };
        let starting_log_sequence = authority.next_log_sequence;
        let (result, durably_empty) = match operation(authority, &appender) {
            Ok(value)
                if reconcile_appended_source
                    && authority.next_log_sequence > starting_log_sequence =>
            {
                // The v2 source barrier crossed. Reconcile its exact Unit 2
                // projection under this same conversation lock before the
                // caller can publish the correlated terminal response.
                match self.replay_and_repair(conversation_id, &log) {
                    Ok(reconciled) => {
                        let durably_empty = reconciled.next_log_sequence == 0;
                        *owner = Some(reconciled);
                        (Ok(value), durably_empty)
                    }
                    Err(error) => {
                        *owner = None;
                        (Err(error), false)
                    }
                }
            }
            Ok(value) => {
                let durably_empty = authority.next_log_sequence == 0;
                (Ok(value), durably_empty)
            }
            Err(error) => {
                // Discard the possibly part-consumed in-memory owner; durable
                // reality is authoritative and will be replayed next touch.
                let durably_empty = authority.next_log_sequence == 0;
                *owner = None;
                (Err(state_error(&error)), durably_empty)
            }
        };
        drop(owner);
        if durably_empty {
            self.evict_uncommitted(conversation_id, &cell)?;
        }
        result
    }

    /// Cold-replays one conversation's durable log and repairs its observer
    /// registration.
    ///
    /// An enrolled conversation whose durable observer `Track` row was lost
    /// to a crash between the enrollment append and the tracking append is
    /// re-registered idempotently here, so observer recovery is derivable
    /// from the conversation log itself on any first touch.
    pub(super) fn replay_and_repair(
        &self,
        conversation_id: ConversationId,
        log: &OperationLog,
    ) -> Result<ConversationAuthority, ParticipantSemanticError> {
        let outbox_log = OutboxLog::new(Arc::clone(&self.store), conversation_id);
        block_on(outbox_log.restore_cursor().validate_all())
            .map_err(|error| bridge_error(&error))?
            .map_err(|error| outbox_log_error(&error))?;
        let mut replayed = block_on(ConversationAuthority::replay(
            conversation_id,
            log,
            &outbox_log,
            &self.config,
            self.outbox_limits,
        ))
        .map_err(|error| bridge_error(&error))?
        .map_err(|error| match error {
            RestoreError::Extension(error) => outbox_log_error(&error),
            RestoreError::Semantic(error) => state_error(&error),
        })?;
        let appender = LogAppender {
            log,
            registry: &self.registry,
            conversation_id,
        };
        replayed
            .repair_pending_specific_fates(&appender)
            .map_err(|error| state_error(&error))?;
        let observer_witnesses = replayed.take_observer_progress_witnesses();
        if !replayed.tokens.is_empty() {
            self.reconcile_observer_progress(
                conversation_id,
                &observer_witnesses,
                replayed.observer_progress,
            )?;
        } else if !observer_witnesses.is_empty() {
            return Err(ParticipantSemanticError::Internal {
                message: format!(
                    "unenrolled conversation {conversation_id} projected observer progress"
                ),
            });
        }
        // Request-time expiry over the replayed state (replay rebuilds every
        // retired rotation's fingerprint; the ones past their deadlines are
        // dropped under this touch's clock read), then fold the
        // conversation's complete server-scope contribution into the ledger
        // — a replace, so a discarded-and-replayed owner never double
        // counts and the ledger self-heals from durable truth.
        let now = facts::now_unix_millis().map_err(|error| ParticipantSemanticError::Internal {
            message: format!("participant clock read failed: {error}"),
        })?;
        let now = u128::from(now);
        replayed.prune_expired_provenance(now);
        let contribution = replayed
            .capacity_contribution(now)
            .map_err(|error| state_error(&error))?;
        self.capacity
            .fold_conversation(conversation_id, contribution)
            .map_err(|error| state_error(&error))?;
        Ok(replayed)
    }

    /// Runs the frozen pre-W3 aggregate reference without installing any
    /// owner, observer, capacity, or publication state.
    #[cfg(test)]
    pub(super) fn replay_aggregate_reference(
        &self,
        conversation_id: ConversationId,
        log: &OperationLog,
    ) -> Result<ConversationAuthority, ParticipantSemanticError> {
        let outbox_log = OutboxLog::new(Arc::clone(&self.store), conversation_id);
        let extension_rows = block_on(outbox_log.read_all())
            .map_err(|error| bridge_error(&error))?
            .map_err(|error| outbox_log_error(&error))?;
        block_on(ConversationAuthority::replay_aggregate_reference(
            conversation_id,
            log,
            &outbox_log,
            extension_rows,
            &self.config,
            self.outbox_limits,
        ))
        .map_err(|error| bridge_error(&error))?
        .map_err(|error| state_error(&error))
    }

    /// Removes a conversation's registry cell after a durably empty touch.
    ///
    /// Only the exact cell this operation used is removed (a racing request
    /// may have installed a fresh cell already); a concurrent holder of the
    /// evicted cell stays correct because every durable append is optimistic
    /// on its exact sequence and cold replay is the source of truth.
    pub(super) fn evict_uncommitted(
        &self,
        conversation_id: ConversationId,
        cell: &Arc<Mutex<Option<ConversationAuthority>>>,
    ) -> Result<(), ParticipantSemanticError> {
        let mut conversations =
            self.conversations
                .lock()
                .map_err(|_| ParticipantSemanticError::Internal {
                    message: "participant conversation registry lock is poisoned".to_owned(),
                })?;
        if let Some(existing) = conversations.get(&conversation_id) {
            if Arc::ptr_eq(existing, cell) {
                conversations.remove(&conversation_id);
            }
        }
        drop(conversations);
        Ok(())
    }

    /// Number of live conversation registry cells (test observability).
    #[cfg(test)]
    pub(super) fn registry_len(&self) -> usize {
        self.conversations
            .lock()
            .map_or(usize::MAX, |conversations| conversations.len())
    }

    /// Drops only volatile participant/observer owners for cold-first-touch tests.
    #[cfg(test)]
    pub(super) fn discard_owners_for_test(&self) -> Result<(), ParticipantSemanticError> {
        self.conversations
            .lock()
            .map_err(|_| ParticipantSemanticError::Internal {
                message: "participant conversation registry lock is poisoned".to_owned(),
            })?
            .clear();
        self.observer
            .lock()
            .map_err(|_| ParticipantSemanticError::Internal {
                message: "observer recovery aggregate lock is poisoned".to_owned(),
            })?
            .take();
        Ok(())
    }

    pub(super) fn operation_facts(
        &self,
        context: ParticipantConnectionContext,
        conversation_id: ConversationId,
        conversations: &ParticipantConnectionConversations,
    ) -> Result<OperationFacts, ParticipantSemanticError> {
        let now_ms =
            facts::now_unix_millis().map_err(|error| ParticipantSemanticError::Internal {
                message: format!("participant clock read failed: {error}"),
            })?;
        // The connection map only grows through capacity commits, so its
        // occupancy always fits the validated nonzero signed limit; a counter
        // rejection here is genuine internal drift and fails closed.
        let connection_capacity = CapacityCounter::try_new(
            self.config.max_semantic_conversations_per_connection,
            conversations.occupied(),
        )
        .map_err(|error| ParticipantSemanticError::Internal {
            message: format!(
                "connection-conversation occupancy disagrees with its signed limit: {error:?}"
            ),
        })?;
        Ok(OperationFacts {
            receiving_incarnation: context.connection_incarnation(),
            now_ms,
            identity_slots: self.config.identity_slots,
            attach_receipt_ttl_ms: self.config.attach_receipt_ttl_ms,
            receipt_provenance_ttl_ms: self.config.receipt_provenance_ttl_ms,
            receipt_limits: ReceiptCapacityLimits {
                identity_server: self.config.max_retired_identity_slots_server,
                live_receipts_server: self.config.max_live_attach_receipts_server,
                live_receipts_per_participant: self.config.max_live_attach_receipts_per_participant,
                provenance_server: self.config.max_receipt_provenance_server,
                provenance_per_conversation: self.config.max_receipt_provenance_per_conversation,
                provenance_per_participant: self.config.max_receipt_provenance_per_participant,
            },
            connection_tracking: conversations.tracking(conversation_id),
            connection_capacity,
        })
    }
}

/// Bridges the synchronous state seam onto the async durable log, and keeps
/// the conversation registry complete by construction: the one
/// conversation-creating append (genesis at sequence zero) is preceded by a
/// durable registry row, so startup can enumerate every conversation stream
/// that exists.
struct LogAppender<'a> {
    log: &'a OperationLog,
    registry: &'a ConversationRegistry,
    conversation_id: ConversationId,
}

impl DurableAppend for LogAppender<'_> {
    fn append(
        &self,
        operation: &StoredOperation,
        expected_sequence: u64,
    ) -> Result<(), OperationLogError> {
        if expected_sequence == 0 && matches!(operation, StoredOperation::Genesis { .. }) {
            self.registry.register(self.conversation_id)?;
        }
        block_on(self.log.append(operation, expected_sequence))?
    }
}

pub(super) fn state_error(error: &StateError) -> ParticipantSemanticError {
    ParticipantSemanticError::Internal {
        message: format!("participant production operation failed: {error}"),
    }
}

pub(super) fn log_error(error: &OperationLogError) -> ParticipantSemanticError {
    ParticipantSemanticError::Internal {
        message: format!("participant production log failed: {error}"),
    }
}

pub(super) fn outbox_log_error(error: &OutboxLogError) -> ParticipantSemanticError {
    ParticipantSemanticError::Internal {
        message: format!("participant Unit 2 extension log failed: {error}"),
    }
}

pub(super) fn bridge_error(
    error: &liminal::durability::bridge::BridgeError,
) -> ParticipantSemanticError {
    ParticipantSemanticError::Internal {
        message: format!("participant durability bridge failed: {error}"),
    }
}
