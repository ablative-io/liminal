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
use liminal_protocol::lifecycle::{
    CapacityCounter, ObserverProgressTrackDecision, ObserverRecoveryAggregate,
    ObserverRecoveryTransactionDecision,
};
use liminal_protocol::wire::{
    ClientRequest, ConversationId, ObserverRecoveryHandshake, ObserverRecoveryResponse, ServerValue,
};

use crate::config::types::ParticipantConfig;
use crate::server::participant::{
    ParticipantConnectionContext, ParticipantConnectionConversations, ParticipantSemanticError,
    ParticipantSemanticHandler,
};

use super::barrier::{ArmOutcome, OperationFacts};
use super::facts;
use super::log::{OperationLog, OperationLogError, StoredOperation};
use super::observer::{ObserverLog, ObserverRow};
use super::state::{ConversationAuthority, DurableAppend, StateError};

/// Production semantic handler backed by the shared durable store.
///
/// Constructed exactly once per server by the connection-services layer when
/// the deployment's `[participant]` configuration is present.
#[derive(Debug)]
pub struct ProductionParticipantHandler {
    store: Arc<dyn DurableStore>,
    config: ParticipantConfig,
    conversations: Mutex<HashMap<ConversationId, Arc<Mutex<Option<ConversationAuthority>>>>>,
    /// Server-wide observer-recovery aggregate paired with its durable row
    /// head (`None` until first restored).
    observer: Mutex<Option<(ObserverRecoveryAggregate, u64)>>,
}

impl ProductionParticipantHandler {
    /// Creates the handler over the server's shared durable store.
    #[must_use]
    pub fn new(store: Arc<dyn DurableStore>, config: ParticipantConfig) -> Self {
        Self {
            store,
            config,
            conversations: Mutex::new(HashMap::new()),
            observer: Mutex::new(None),
        }
    }

    fn cell(
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
        let appender = LogAppender { log: &log };
        let (result, durably_empty) = match operation(authority, &appender) {
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
    fn replay_and_repair(
        &self,
        conversation_id: ConversationId,
        log: &OperationLog,
    ) -> Result<ConversationAuthority, ParticipantSemanticError> {
        let replayed = block_on(ConversationAuthority::replay(conversation_id, log))
            .map_err(|error| bridge_error(&error))?
            .map_err(|error| state_error(&error))?;
        if !replayed.tokens.is_empty() {
            self.ensure_observer_tracked(conversation_id)?;
        }
        Ok(replayed)
    }

    /// Removes a conversation's registry cell after a durably empty touch.
    ///
    /// Only the exact cell this operation used is removed (a racing request
    /// may have installed a fresh cell already); a concurrent holder of the
    /// evicted cell stays correct because every durable append is optimistic
    /// on its exact sequence and cold replay is the source of truth.
    fn evict_uncommitted(
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
            connection_tracking: conversations.tracking(conversation_id),
            connection_capacity,
        })
    }

    /// Applies one observer-recovery batch through the A4 atomic transaction.
    ///
    /// The aggregate is owned by the transaction between the progress read
    /// and the arm installation, and the whole arm plan is durably appended
    /// before installation — a crash leaves the complete plan or none.
    fn apply_observer_recovery(
        &self,
        conversations: &mut ParticipantConnectionConversations,
        request: &ObserverRecoveryHandshake,
    ) -> Result<ServerValue, ParticipantSemanticError> {
        // Crash-window repair pre-pass: every named conversation's observer
        // registration is made derivable from its own durable conversation
        // log before the batch classifies, so an enrollment whose Track row
        // was lost between the two appends is re-registered idempotently
        // instead of being refused forever.
        for refusal in &request.observer_refusals {
            self.ensure_tracking_from_log(refusal.conversation_id)?;
        }
        // Connection-conversation occupancy is the SAME connection-local
        // dispatch map the semantic stage-6 gate consumes, so the batch
        // preflight and every semantic operation count against one signed
        // bound.
        let tracked = conversations.tracked_conversations();
        let observer_log = ObserverLog::new(Arc::clone(&self.store));
        let mut owner = self
            .observer
            .lock()
            .map_err(|_| ParticipantSemanticError::Internal {
                message: "observer recovery aggregate lock is poisoned".to_owned(),
            })?;
        if owner.is_none() {
            let restored = block_on(observer_log.restore())
                .map_err(|error| bridge_error(&error))?
                .map_err(|error| log_error(&error))?;
            *owner = Some((restored.aggregate, restored.next_sequence));
        }
        let (aggregate, head) = owner
            .take()
            .ok_or_else(|| ParticipantSemanticError::Internal {
                message: "observer recovery aggregate is absent".to_owned(),
            })?;
        let result = match aggregate.decide_recovery(
            request,
            self.config.observer_recovery_max_entries,
            self.config.max_semantic_conversations_per_connection,
            &tracked,
        ) {
            ObserverRecoveryTransactionDecision::Respond {
                aggregate,
                response,
            } => {
                *owner = Some((aggregate, head));
                Ok(response.into_server_value())
            }
            ObserverRecoveryTransactionDecision::Commit(transaction) => {
                let arms = transaction
                    .arms()
                    .iter()
                    .map(|arm| (arm.conversation_id(), arm.refused_epoch()))
                    .collect::<Vec<_>>();
                match block_on(observer_log.append(&ObserverRow::Arms { arms: arms.clone() }, head))
                    .map_err(|error| bridge_error(&error))?
                {
                    Ok(()) => {
                        let (aggregate, outcome) = transaction.commit();
                        *owner = Some((aggregate, head.saturating_add(1)));
                        // Every armed refusal-only recipient occupies one
                        // connection-conversation slot (the batch preflight
                        // already admitted them against the signed bound).
                        for (conversation_id, _) in arms {
                            conversations.track(conversation_id);
                        }
                        Ok(ObserverRecoveryResponse::accepted(outcome).into_server_value())
                    }
                    Err(error) => {
                        // Nothing installed; the aggregate is durably restored
                        // on the next observer operation.
                        Err(state_error(&StateError::Log(error)))
                    }
                }
            }
        };
        drop(owner);
        result
    }

    /// Registers a conversation's observer progress row on first touch.
    ///
    /// Idempotent: an already-tracked conversation returns unchanged.
    fn ensure_observer_tracked(
        &self,
        conversation_id: ConversationId,
    ) -> Result<(), ParticipantSemanticError> {
        let observer_log = ObserverLog::new(Arc::clone(&self.store));
        let mut owner = self
            .observer
            .lock()
            .map_err(|_| ParticipantSemanticError::Internal {
                message: "observer recovery aggregate lock is poisoned".to_owned(),
            })?;
        if owner.is_none() {
            let restored = block_on(observer_log.restore())
                .map_err(|error| bridge_error(&error))?
                .map_err(|error| log_error(&error))?;
            *owner = Some((restored.aggregate, restored.next_sequence));
        }
        let (aggregate, head) = owner
            .take()
            .ok_or_else(|| ParticipantSemanticError::Internal {
                message: "observer recovery aggregate is absent".to_owned(),
            })?;
        let result = match aggregate.decide_track(conversation_id, 0) {
            ObserverProgressTrackDecision::Refuse { aggregate, .. } => {
                // Already tracked — the registration is durable.
                *owner = Some((aggregate, head));
                Ok(())
            }
            ObserverProgressTrackDecision::Commit(transaction) => {
                match block_on(observer_log.append(
                    &ObserverRow::Track {
                        conversation_id,
                        observer_progress: 0,
                    },
                    head,
                ))
                .map_err(|error| bridge_error(&error))?
                {
                    Ok(()) => {
                        *owner = Some((transaction.commit(), head.saturating_add(1)));
                        Ok(())
                    }
                    Err(error) => Err(state_error(&StateError::Log(error))),
                }
            }
        };
        drop(owner);
        result
    }

    /// Ensures a conversation's observer registration is derivable from its
    /// own durable log (idempotent crash-window repair).
    ///
    /// A never-committed conversation id leaves no residue: its probe cell is
    /// evicted. An already-live enrolled owner re-registers idempotently,
    /// covering an earlier failed Track append inside this process lifetime.
    fn ensure_tracking_from_log(
        &self,
        conversation_id: ConversationId,
    ) -> Result<(), ParticipantSemanticError> {
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
            if replayed.next_log_sequence == 0 {
                drop(owner);
                return self.evict_uncommitted(conversation_id, &cell);
            }
            *owner = Some(replayed);
            drop(owner);
            return Ok(());
        }
        let enrolled = owner
            .as_ref()
            .is_some_and(|authority| !authority.tokens.is_empty());
        drop(owner);
        if enrolled {
            self.ensure_observer_tracked(conversation_id)?;
        }
        Ok(())
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
                        authority.apply_enrollment(&request, &operation_facts, appender)
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
                        authority.apply_credential_attach(&request, &operation_facts, appender)
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
                    |authority, _appender| authority.apply_leave(&request, &operation_facts),
                )
            }
            ClientRequest::RecordAdmission(request) => {
                let operation_facts =
                    self.operation_facts(context, request.conversation_id, conversations)?;
                self.conversation_operation(
                    request.conversation_id,
                    conversations,
                    |authority, _appender| {
                        authority.apply_record_admission(&request, &operation_facts)
                    },
                )
            }
            ClientRequest::ObserverRecovery(request) => {
                self.apply_observer_recovery(conversations, &request)
            }
        }
    }
}

/// Bridges the synchronous state seam onto the async durable log.
struct LogAppender<'a> {
    log: &'a OperationLog,
}

impl DurableAppend for LogAppender<'_> {
    fn append(
        &self,
        operation: &StoredOperation,
        expected_sequence: u64,
    ) -> Result<(), OperationLogError> {
        block_on(self.log.append(operation, expected_sequence))?
    }
}

fn state_error(error: &StateError) -> ParticipantSemanticError {
    ParticipantSemanticError::Internal {
        message: format!("participant production operation failed: {error}"),
    }
}

fn log_error(error: &OperationLogError) -> ParticipantSemanticError {
    ParticipantSemanticError::Internal {
        message: format!("participant production log failed: {error}"),
    }
}

fn bridge_error(error: &liminal::durability::bridge::BridgeError) -> ParticipantSemanticError {
    ParticipantSemanticError::Internal {
        message: format!("participant durability bridge failed: {error}"),
    }
}
