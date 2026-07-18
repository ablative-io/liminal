//! Participant publication and semantic request entry points.

use std::collections::BTreeSet;
use std::sync::Arc;

use liminal_protocol::lifecycle::BindingState;
use liminal_protocol::wire::{
    BindingEpoch, ClientRequest, ConnectionIncarnation, ConversationId, EnrollmentRequest,
    ObserverRecoveryHandshake, ParticipantId, ParticipantRecord, ServerValue,
};

use crate::server::participant::{
    ObserverPublicationTarget, ParticipantConnectionContext, ParticipantConnectionConversations,
    ParticipantOfferedProgress, ParticipantPublication, ParticipantSemanticError,
    ParticipantSemanticHandler,
};

use super::barrier::ArmOutcome;
use super::handler::ProductionParticipantHandler;
use super::outbox_log::OutboxLog;
use super::state::{ConversationAuthority, DurableAppend, StateError};

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

    fn handle_enrollment(
        &self,
        context: ParticipantConnectionContext,
        conversations: &mut ParticipantConnectionConversations,
        request: &EnrollmentRequest,
    ) -> Result<ServerValue, ParticipantSemanticError> {
        let operation_facts =
            self.operation_facts(context, request.conversation_id, conversations)?;
        let value = self.conversation_operation(
            request.conversation_id,
            conversations,
            |authority, appender| {
                authority.apply_enrollment(
                    request,
                    &operation_facts,
                    &self.capacity,
                    &self.config,
                    appender,
                )
            },
        )?;
        // Only a fresh commit registers observer tracking; refusals and replays
        // leave the observer log untouched.
        if matches!(value, ServerValue::EnrollBound(_)) {
            self.ensure_observer_tracked(request.conversation_id)?;
        }
        Ok(value)
    }
}

impl ParticipantSemanticHandler for ProductionParticipantHandler {
    fn publication_conversation_limit(&self) -> u64 {
        self.config.max_semantic_conversations_per_connection
    }

    fn ready_connection_incarnations(
        &self,
        conversation_id: ConversationId,
    ) -> Result<Vec<ConnectionIncarnation>, ParticipantSemanticError> {
        let cell = self.cell(conversation_id)?;
        let owner = cell
            .lock()
            .map_err(|_| publication_owner_poisoned(conversation_id))?;
        let Some(authority) = owner.as_ref() else {
            return Ok(Vec::new());
        };
        let Some(outbox) = authority.outbox.as_ref() else {
            return Err(publication_owner_missing(conversation_id));
        };
        let mut incarnations = BTreeSet::new();
        for participant_id in outbox.live_recipients() {
            let Some(slot) = authority.slots.get(&participant_id) else {
                continue;
            };
            if let BindingState::Bound(active) = &slot.binding {
                incarnations.insert(active.binding_epoch.connection_incarnation);
            }
        }
        let incarnations = incarnations.into_iter().collect();
        drop(owner);
        Ok(incarnations)
    }

    fn next_publication(
        &self,
        connection_incarnation: ConnectionIncarnation,
        conversation_id: ConversationId,
        offered: Option<ParticipantOfferedProgress>,
    ) -> Result<Option<ParticipantPublication>, ParticipantSemanticError> {
        let cell = self.cell(conversation_id)?;
        let owner = cell
            .lock()
            .map_err(|_| publication_owner_poisoned(conversation_id))?;
        let Some(authority) = owner.as_ref() else {
            return Ok(None);
        };
        let bound = authority.slots.iter().find_map(|(participant_id, slot)| {
            let BindingState::Bound(active) = &slot.binding else {
                return None;
            };
            (active.binding_epoch.connection_incarnation == connection_incarnation)
                .then_some((*participant_id, active.binding_epoch))
        });
        let Some((participant_id, binding_epoch)) = bound else {
            return Ok(None);
        };
        let Some(outbox) = authority.outbox.as_ref() else {
            return Err(publication_owner_missing(conversation_id));
        };
        let offered_through = offered
            .filter(|progress| progress.binding_epoch == binding_epoch)
            .map_or_else(
                || outbox.durable_ack_through(participant_id),
                |progress| progress.through_seq,
            );
        let publication = outbox
            .delivery_after(participant_id, offered_through)
            .map(|delivery| ParticipantPublication {
                participant_id,
                binding_epoch,
                delivery,
            });
        drop(owner);
        Ok(publication)
    }

    fn publication_binding_is_current(
        &self,
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        binding_epoch: BindingEpoch,
    ) -> Result<bool, ParticipantSemanticError> {
        let cell = self.cell(conversation_id)?;
        let owner = cell
            .lock()
            .map_err(|_| publication_owner_poisoned(conversation_id))?;
        let Some(authority) = owner.as_ref() else {
            return Ok(false);
        };
        let current = authority.slots.get(&participant_id).is_some_and(|slot| {
            matches!(
                &slot.binding,
                BindingState::Bound(active) if active.binding_epoch == binding_epoch
            )
        });
        drop(owner);
        Ok(current)
    }

    fn record_publication_offer(
        &self,
        publication: &ParticipantPublication,
    ) -> Result<(), ParticipantSemanticError> {
        if !matches!(
            publication.delivery.record,
            ParticipantRecord::HistoryCompacted { .. }
        ) {
            return Ok(());
        }
        let conversation_id = publication.conversation_id();
        let cell = self.cell(conversation_id)?;
        let mut owner = cell
            .lock()
            .map_err(|_| publication_owner_poisoned(conversation_id))?;
        let Some(authority) = owner.as_mut() else {
            return Err(publication_owner_missing(conversation_id));
        };
        let current = authority
            .slots
            .get(&publication.participant_id)
            .is_some_and(|slot| {
                matches!(
                    &slot.binding,
                    BindingState::Bound(active)
                        if active.binding_epoch == publication.binding_epoch
                )
            });
        let obligation = authority.outbox.as_ref().is_some_and(|outbox| {
            outbox.is_marker_obligation(publication.participant_id, publication.delivery_seq())
        });
        if !current || !obligation {
            return Err(ParticipantSemanticError::Internal {
                message: format!(
                    "participant marker offer lost its exact current binding or durable obligation for conversation {conversation_id}"
                ),
            });
        }
        authority.offered_markers.insert(
            (publication.participant_id, publication.delivery_seq()),
            publication.binding_epoch,
        );
        drop(owner);
        Ok(())
    }

    fn handle_observer_recovery(
        &self,
        context: ParticipantConnectionContext,
        conversations: &mut ParticipantConnectionConversations,
        request: ObserverRecoveryHandshake,
        target: Option<ObserverPublicationTarget>,
    ) -> Result<ServerValue, ParticipantSemanticError> {
        self.apply_observer_recovery(context, conversations, &request, target)
    }

    fn handle(
        &self,
        context: ParticipantConnectionContext,
        conversations: &mut ParticipantConnectionConversations,
        request: ClientRequest,
    ) -> Result<ServerValue, ParticipantSemanticError> {
        match request {
            ClientRequest::Enrollment(request) => {
                self.handle_enrollment(context, conversations, &request)
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
                let outbox_log = OutboxLog::new(Arc::clone(&self.store), request.conversation_id);
                self.conversation_operation(
                    request.conversation_id,
                    conversations,
                    |authority, _appender| {
                        authority.apply_marker_ack(&request, &operation_facts, &outbox_log)
                    },
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
                self.apply_observer_recovery(context, conversations, &request, None)
            }
        }
    }
}

fn publication_owner_poisoned(conversation_id: ConversationId) -> ParticipantSemanticError {
    ParticipantSemanticError::Internal {
        message: format!(
            "participant conversation {conversation_id} owner lock is poisoned during publication"
        ),
    }
}

fn publication_owner_missing(conversation_id: ConversationId) -> ParticipantSemanticError {
    ParticipantSemanticError::Internal {
        message: format!(
            "participant conversation {conversation_id} publication owner is unavailable"
        ),
    }
}
