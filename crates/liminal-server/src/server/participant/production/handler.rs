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

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use liminal::durability::DurableStore;
use liminal::durability::bridge::block_on;
use liminal_protocol::lifecycle::{CapacityCounter, ObserverRecoveryAggregate};
use liminal_protocol::wire::{ClientRequest, ConversationId, ServerValue};

use crate::config::types::ParticipantConfig;
use crate::server::participant::{
    ParticipantConnectionContext, ParticipantConnectionConversations, ParticipantSemanticError,
    ParticipantSemanticHandler,
};

use super::barrier::{ArmOutcome, OperationFacts, ReceiptCapacityLimits};
use super::capacity::ServerCapacity;
use super::facts;
use super::log::{OperationLog, OperationLogError, StoredOperation};
use super::outbox_log::{OutboxLog, OutboxLogError};
use super::registry::ConversationRegistry;
use super::state::{ConversationAuthority, DurableAppend, StateError};

/// Production semantic handler backed by the shared durable store.
///
/// Constructed exactly once per server by the connection-services layer when
/// the deployment's `[participant]` configuration is present.
#[derive(Debug)]
pub struct ProductionParticipantHandler {
    pub(super) store: Arc<dyn DurableStore>,
    pub(super) config: ParticipantConfig,
    conversations: Mutex<HashMap<ConversationId, Arc<Mutex<Option<ConversationAuthority>>>>>,
    /// Server-wide observer-recovery aggregate paired with its durable row
    /// head (`None` until first restored).
    pub(super) observer: Mutex<Option<(ObserverRecoveryAggregate, u64)>>,
    /// Server-scope stage-8 occupancy ledger (identity slots, live receipts,
    /// provenance fingerprints), restored from every durable conversation at
    /// construction and kept exact by commit reservations and replay folds.
    capacity: ServerCapacity,
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
        let registry = ConversationRegistry::new(Arc::clone(&store));
        let handler = Self {
            store,
            config,
            conversations: Mutex::new(HashMap::new()),
            observer: Mutex::new(None),
            capacity: ServerCapacity::default(),
            registry,
        };
        handler.restore_all_conversations()?;
        Ok(handler)
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
    fn with_conversation(
        &self,
        conversation_id: ConversationId,
        operation: impl FnOnce(
            &mut ConversationAuthority,
            &dyn DurableAppend,
        ) -> Result<ArmOutcome, StateError>,
    ) -> Result<ArmOutcome, ParticipantSemanticError> {
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
            Ok(value) if authority.next_log_sequence > starting_log_sequence => {
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
        let extension_rows = block_on(outbox_log.read_all())
            .map_err(|error| bridge_error(&error))?
            .map_err(|error| outbox_log_error(&error))?;
        let mut replayed = block_on(ConversationAuthority::replay(
            conversation_id,
            log,
            &outbox_log,
            extension_rows,
            &self.config,
        ))
        .map_err(|error| bridge_error(&error))?
        .map_err(|error| state_error(&error))?;
        if !replayed.tokens.is_empty() {
            self.ensure_observer_tracked(conversation_id)?;
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

    fn operation_facts(
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

impl ProductionParticipantHandler {
    /// Runs one conversation-scoped operation arm and applies its
    /// connection-tracking effect to the connection's dispatch map.
    fn conversation_operation(
        &self,
        conversation_id: ConversationId,
        conversations: &mut ParticipantConnectionConversations,
        operation: impl FnOnce(
            &mut ConversationAuthority,
            &dyn DurableAppend,
        ) -> Result<ArmOutcome, StateError>,
    ) -> Result<ServerValue, ParticipantSemanticError> {
        let outcome = self.with_conversation(conversation_id, operation)?;
        if outcome.newly_tracked {
            conversations.track(conversation_id);
        }
        Ok(outcome.value)
    }
}

impl ParticipantSemanticHandler for ProductionParticipantHandler {
    fn handle(
        &self,
        context: ParticipantConnectionContext,
        conversations: &mut ParticipantConnectionConversations,
        request: ClientRequest,
    ) -> Result<ServerValue, ParticipantSemanticError> {
        match request {
            ClientRequest::Enrollment(request) => {
                let operation_facts =
                    self.operation_facts(context, request.conversation_id, conversations)?;
                let value = self.conversation_operation(
                    request.conversation_id,
                    conversations,
                    |authority, appender| {
                        authority.apply_enrollment(
                            &request,
                            &operation_facts,
                            &self.capacity,
                            &self.config,
                            appender,
                        )
                    },
                )?;
                // Only a fresh commit registers observer tracking; refusals
                // and replays leave the observer log untouched (an already
                // enrolled conversation was registered at its own commit or
                // by the replay-time repair).
                if matches!(value, ServerValue::EnrollBound(_)) {
                    self.ensure_observer_tracked(request.conversation_id)?;
                }
                Ok(value)
            }
            ClientRequest::CredentialAttach(request) => {
                let operation_facts =
                    self.operation_facts(context, request.conversation_id, conversations)?;
                self.conversation_operation(
                    request.conversation_id,
                    conversations,
                    |authority, appender| {
                        authority.apply_credential_attach(
                            &request,
                            &operation_facts,
                            &self.capacity,
                            appender,
                        )
                    },
                )
            }
            ClientRequest::Detach(request) => {
                let operation_facts =
                    self.operation_facts(context, request.conversation_id, conversations)?;
                self.conversation_operation(
                    request.conversation_id,
                    conversations,
                    |authority, appender| {
                        authority.apply_detach(&request, &operation_facts, appender)
                    },
                )
            }
            ClientRequest::ParticipantAck(request) => {
                let operation_facts =
                    self.operation_facts(context, request.conversation_id, conversations)?;
                self.conversation_operation(
                    request.conversation_id,
                    conversations,
                    |authority, appender| authority.apply_ack(&request, &operation_facts, appender),
                )
            }
            ClientRequest::MarkerAck(request) => {
                let operation_facts =
                    self.operation_facts(context, request.conversation_id, conversations)?;
                self.conversation_operation(
                    request.conversation_id,
                    conversations,
                    |authority, _appender| authority.apply_marker_ack(&request, &operation_facts),
                )
            }
            ClientRequest::Leave(request) => {
                let operation_facts =
                    self.operation_facts(context, request.conversation_id, conversations)?;
                self.conversation_operation(
                    request.conversation_id,
                    conversations,
                    |authority, appender| {
                        authority.apply_leave(&request, &operation_facts, appender)
                    },
                )
            }
            ClientRequest::RecordAdmission(request) => {
                let operation_facts =
                    self.operation_facts(context, request.conversation_id, conversations)?;
                self.conversation_operation(
                    request.conversation_id,
                    conversations,
                    |authority, appender| {
                        authority.apply_record_admission(
                            &request,
                            &operation_facts,
                            &self.config,
                            appender,
                        )
                    },
                )
            }
            ClientRequest::ObserverRecovery(request) => {
                self.apply_observer_recovery(conversations, &request)
            }
        }
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
