//! The single production participant semantic handler.
//!
//! One handler per server. Each conversation has exactly one live in-memory
//! authority owner, rebuilt from its durable transition-input log on first
//! touch (and after any failed operation, which discards the owner so the
//! next touch cold-replays durable reality). A short registry lock selects
//! the conversation cell; a per-conversation lock covers replay and each
//! operation. Everything is event-driven: cells are created on request
//! arrival and discarded on error — no timer, sweep, or polling loop exists.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use liminal::durability::DurableStore;
use liminal::durability::bridge::block_on;
use liminal_protocol::lifecycle::{
    ObserverProgressTrackDecision, ObserverRecoveryAggregate, ObserverRecoveryTransactionDecision,
};
use liminal_protocol::wire::{
    ClientRequest, ConversationId, ObserverRecoveryHandshake, ObserverRecoveryResponse, ServerValue,
};

use crate::config::types::ParticipantConfig;
use crate::server::participant::{
    ParticipantConnectionContext, ParticipantSemanticError, ParticipantSemanticHandler,
};

use super::facts;
use super::log::{OperationLog, OperationLogError, StoredOperation};
use super::observer::{ObserverLog, ObserverRow};
use super::ops_bind::OperationFacts;
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
    /// exact durable reality.
    fn with_conversation(
        &self,
        conversation_id: ConversationId,
        operation: impl FnOnce(
            &mut ConversationAuthority,
            &dyn DurableAppend,
        ) -> Result<ServerValue, StateError>,
    ) -> Result<ServerValue, ParticipantSemanticError> {
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
            let replayed = block_on(ConversationAuthority::replay(conversation_id, &log))
                .map_err(|error| bridge_error(&error))?
                .map_err(|error| state_error(&error))?;
            *owner = Some(replayed);
        }
        let Some(authority) = owner.as_mut() else {
            return Err(ParticipantSemanticError::Internal {
                message: format!("participant conversation {conversation_id} owner is absent"),
            });
        };
        let appender = LogAppender { log: &log };
        let result = match operation(authority, &appender) {
            Ok(value) => Ok(value),
            Err(error) => {
                // Discard the possibly part-consumed in-memory owner; durable
                // reality is authoritative and will be replayed next touch.
                *owner = None;
                Err(state_error(&error))
            }
        };
        drop(owner);
        result
    }

    fn operation_facts(
        &self,
        context: ParticipantConnectionContext,
    ) -> Result<OperationFacts, ParticipantSemanticError> {
        let now_ms =
            facts::now_unix_millis().map_err(|error| ParticipantSemanticError::Internal {
                message: format!("participant clock read failed: {error}"),
            })?;
        Ok(OperationFacts {
            receiving_incarnation: context.connection_incarnation(),
            now_ms,
            identity_slots: self.config.identity_slots,
            attach_receipt_ttl_ms: self.config.attach_receipt_ttl_ms,
            receipt_provenance_ttl_ms: self.config.receipt_provenance_ttl_ms,
        })
    }

    /// Applies one observer-recovery batch through the A4 atomic transaction.
    ///
    /// The aggregate is owned by the transaction between the progress read
    /// and the arm installation, and the whole arm plan is durably appended
    /// before installation — a crash leaves the complete plan or none.
    fn apply_observer_recovery(
        &self,
        context: ParticipantConnectionContext,
        request: &ObserverRecoveryHandshake,
    ) -> Result<ServerValue, ParticipantSemanticError> {
        // Connection-conversation tracking derived from binding authority:
        // the conversations whose live binding epoch names this connection.
        let tracked = self.bound_conversations(context)?;
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
                match block_on(observer_log.append(&ObserverRow::Arms { arms }, head))
                    .map_err(|error| bridge_error(&error))?
                {
                    Ok(()) => {
                        let (aggregate, outcome) = transaction.commit();
                        *owner = Some((aggregate, head.saturating_add(1)));
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

    /// Conversations whose current live binding names this connection.
    fn bound_conversations(
        &self,
        context: ParticipantConnectionContext,
    ) -> Result<Vec<ConversationId>, ParticipantSemanticError> {
        let conversations =
            self.conversations
                .lock()
                .map_err(|_| ParticipantSemanticError::Internal {
                    message: "participant conversation registry lock is poisoned".to_owned(),
                })?;
        let mut bound = Vec::new();
        for (conversation_id, cell) in conversations.iter() {
            let owner = cell
                .lock()
                .map_err(|_| ParticipantSemanticError::Internal {
                    message: format!(
                        "participant conversation {conversation_id} owner lock is poisoned"
                    ),
                })?;
            if let Some(authority) = owner.as_ref() {
                if !matches!(
                    authority.binding_slot_occupancy(context.connection_incarnation()),
                    liminal_protocol::lifecycle::BindingSlotOccupancy::Empty
                ) {
                    bound.push(*conversation_id);
                }
            }
            drop(owner);
        }
        drop(conversations);
        bound.sort_unstable();
        Ok(bound)
    }
}

impl ParticipantSemanticHandler for ProductionParticipantHandler {
    fn handle(
        &self,
        context: ParticipantConnectionContext,
        request: ClientRequest,
    ) -> Result<ServerValue, ParticipantSemanticError> {
        match request {
            ClientRequest::Enrollment(request) => {
                let operation_facts = self.operation_facts(context)?;
                let value =
                    self.with_conversation(request.conversation_id, |authority, appender| {
                        authority.apply_enrollment(&request, &operation_facts, appender)
                    })?;
                self.ensure_observer_tracked(request.conversation_id)?;
                Ok(value)
            }
            ClientRequest::CredentialAttach(request) => {
                let operation_facts = self.operation_facts(context)?;
                self.with_conversation(request.conversation_id, |authority, appender| {
                    authority.apply_credential_attach(&request, &operation_facts, appender)
                })
            }
            ClientRequest::Detach(request) => {
                let operation_facts = self.operation_facts(context)?;
                self.with_conversation(request.conversation_id, |authority, appender| {
                    authority.apply_detach(&request, &operation_facts, appender)
                })
            }
            ClientRequest::ParticipantAck(request) => {
                self.with_conversation(request.conversation_id, |authority, appender| {
                    authority.apply_ack(&request, context.connection_incarnation(), appender)
                })
            }
            ClientRequest::MarkerAck(request) => {
                self.with_conversation(request.conversation_id, |authority, appender| {
                    authority.apply_marker_ack(&request, context.connection_incarnation(), appender)
                })
            }
            ClientRequest::Leave(request) => {
                self.with_conversation(request.conversation_id, |authority, appender| {
                    authority.apply_leave(&request, context.connection_incarnation(), appender)
                })
            }
            ClientRequest::RecordAdmission(request) => {
                let operation_facts = self.operation_facts(context)?;
                self.with_conversation(request.conversation_id, |authority, appender| {
                    authority.apply_record_admission(&request, &operation_facts, appender)
                })
            }
            ClientRequest::ObserverRecovery(request) => {
                self.apply_observer_recovery(context, &request)
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
