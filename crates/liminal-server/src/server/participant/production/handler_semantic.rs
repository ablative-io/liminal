//! Participant publication and semantic request entry points.

use std::collections::BTreeSet;
use std::sync::Arc;

use liminal_protocol::lifecycle::{
    BindingState, ObligationDebtDispatchDecision, decide_obligation_debt_dispatch,
};
use liminal_protocol::wire::{
    BindingEpoch, ClientRequest, ConnectionIncarnation, ConversationId, ObserverRecoveryHandshake,
    ParticipantId, ParticipantRecord, ServerValue,
};

use crate::server::participant::{
    ConnectionFateWorkItem, ObserverPublicationTarget, ParticipantConnectionContext,
    ParticipantConnectionConversations, ParticipantConnectionFateOutcome,
    ParticipantOfferedProgress, ParticipantPublication, ParticipantSemanticError,
    ParticipantSemanticHandler, ParticipantSemanticOutcome, ParticipantServiceFatal,
    dispatch_impact::DispatchImpactAccumulator,
};

use super::barrier::{ArmOutcome, OperationFacts};
use super::handler::ProductionParticipantHandler;
use super::outbox_log::OutboxLog;
use super::state::{ConversationAuthority, DurableAppend, StateError};

impl ProductionParticipantHandler {
    /// Runs one conversation-scoped operation with one lossless request impact
    /// accumulator under the conversation lock, then applies its tracking effect.
    fn conversation_operation_with_impact(
        &self,
        conversation_id: ConversationId,
        conversations: &mut ParticipantConnectionConversations,
        operation: impl FnOnce(
            &mut ConversationAuthority,
            &dyn DurableAppend,
            &mut DispatchImpactAccumulator,
        ) -> Result<ArmOutcome, StateError>,
    ) -> ParticipantSemanticOutcome<ServerValue> {
        let mut impact = DispatchImpactAccumulator::new();
        let result = self.with_conversation_impact(conversation_id, &mut impact, operation);
        let result = result.map(|outcome| {
            if outcome.newly_tracked {
                conversations.track(conversation_id);
            }
            outcome.value
        });
        ParticipantSemanticOutcome::new(result, impact.finish(conversation_id))
    }

    fn conversation_operation_with_facts(
        &self,
        context: ParticipantConnectionContext,
        conversation_id: ConversationId,
        conversations: &mut ParticipantConnectionConversations,
        operation: impl FnOnce(
            &mut ConversationAuthority,
            &OperationFacts,
            &dyn DurableAppend,
            &mut DispatchImpactAccumulator,
        ) -> Result<ArmOutcome, StateError>,
    ) -> ParticipantSemanticOutcome<ServerValue> {
        let operation_facts = match self.operation_facts(context, conversation_id, conversations) {
            Ok(facts) => facts,
            Err(error) => return ParticipantSemanticOutcome::unchanged(Err(error)),
        };
        self.conversation_operation_with_impact(
            conversation_id,
            conversations,
            |authority, appender, impact| operation(authority, &operation_facts, appender, impact),
        )
    }

    fn handle_request_with_impact(
        &self,
        context: ParticipantConnectionContext,
        conversations: &mut ParticipantConnectionConversations,
        request: ClientRequest,
    ) -> ParticipantSemanticOutcome<ServerValue> {
        if let Err(error) = self.ensure_service_live() {
            return ParticipantSemanticOutcome::unchanged(Err(error));
        }
        match request {
            ClientRequest::Enrollment(request) => self.conversation_operation_with_facts(
                context,
                request.conversation_id,
                conversations,
                |authority, operation_facts, appender, impact| {
                    authority.apply_enrollment_with_impact(
                        &request,
                        operation_facts,
                        &self.capacity,
                        &self.config,
                        appender,
                        impact,
                    )
                },
            ),
            ClientRequest::CredentialAttach(request) => self.conversation_operation_with_facts(
                context,
                request.conversation_id,
                conversations,
                |authority, operation_facts, appender, impact| {
                    authority.apply_credential_attach_with_impact(
                        &request,
                        operation_facts,
                        &self.capacity,
                        self.store.clone(),
                        appender,
                        impact,
                    )
                },
            ),
            ClientRequest::Detach(request) => self.conversation_operation_with_facts(
                context,
                request.conversation_id,
                conversations,
                |authority, operation_facts, appender, impact| {
                    authority.apply_detach_with_impact(&request, operation_facts, appender, impact)
                },
            ),
            ClientRequest::ParticipantAck(request) => self.conversation_operation_with_facts(
                context,
                request.conversation_id,
                conversations,
                |authority, operation_facts, appender, impact| {
                    authority.apply_ack_with_impact(&request, operation_facts, appender, impact)
                },
            ),
            ClientRequest::MarkerAck(request) => {
                let outbox_log = OutboxLog::new(Arc::clone(&self.store), request.conversation_id);
                self.conversation_operation_with_facts(
                    context,
                    request.conversation_id,
                    conversations,
                    |authority, operation_facts, _appender, impact| {
                        authority.apply_marker_ack_with_impact(
                            &request,
                            operation_facts,
                            &outbox_log,
                            impact,
                        )
                    },
                )
            }
            ClientRequest::Leave(request) => self.conversation_operation_with_facts(
                context,
                request.conversation_id,
                conversations,
                |authority, operation_facts, appender, impact| {
                    authority.apply_leave_with_impact(&request, operation_facts, appender, impact)
                },
            ),
            ClientRequest::RecordAdmission(request) => self.conversation_operation_with_facts(
                context,
                request.conversation_id,
                conversations,
                |authority, operation_facts, appender, impact| {
                    authority.apply_record_admission_with_impact(
                        &request,
                        operation_facts,
                        &self.config,
                        appender,
                        impact,
                    )
                },
            ),
            ClientRequest::ObserverRecovery(request) => ParticipantSemanticOutcome::unchanged(
                self.apply_observer_recovery(context, conversations, &request, None),
            ),
        }
    }
}

impl ParticipantSemanticHandler for ProductionParticipantHandler {
    fn service_fatal(&self) -> Result<Option<ParticipantServiceFatal>, ParticipantSemanticError> {
        self.current_service_fatal()
    }

    fn latch_connection_fate_intent_incomplete(
        &self,
        open_sequence: u64,
        conversation_id: ConversationId,
    ) -> Result<ParticipantServiceFatal, ParticipantSemanticError> {
        self.latch_connection_fate_fatal(open_sequence, conversation_id)
    }

    fn handle_connection_fate(
        &self,
        work_item: ConnectionFateWorkItem,
    ) -> Result<(), ParticipantSemanticError> {
        self.apply_connection_fate_with_impacts(&work_item)
            .into_result()
    }

    fn handle_connection_fate_with_impact(
        &self,
        work_item: ConnectionFateWorkItem,
    ) -> ParticipantConnectionFateOutcome {
        self.apply_connection_fate_with_impacts(&work_item)
    }

    fn repair_unclean_server_restart(
        &self,
        current_server_incarnation: u64,
    ) -> Result<(), ParticipantSemanticError> {
        self.ensure_service_live()?;
        for conversation_id in self.registered_conversation_ids()? {
            self.with_conversation_fate_source(
                conversation_id,
                None,
                |authority, appender, impact| {
                    if impact.is_some() {
                        return Err(StateError::invariant(
                            "startup connection-fate repair gained a dispatch impact owner",
                        ));
                    }
                    let transaction = authority
                        .prepare_unclean_server_restart_transaction(current_server_incarnation)?;
                    transaction.complete(authority, appender)
                },
            )?;
        }
        Ok(())
    }

    fn connection_has_bound_participant(
        &self,
        connection_incarnation: ConnectionIncarnation,
        conversations: &[ConversationId],
    ) -> Result<bool, ParticipantSemanticError> {
        self.ensure_service_live()?;
        for conversation_id in conversations {
            let cell = self.cell(*conversation_id)?;
            let owner = cell
                .lock()
                .map_err(|_| publication_owner_poisoned(*conversation_id))?;
            let Some(authority) = owner.as_ref() else {
                drop(owner);
                continue;
            };
            let bound = authority.slots.values().any(|slot| {
                matches!(
                    slot.binding,
                    BindingState::Bound(active)
                        if active.binding_epoch.connection_incarnation == connection_incarnation
                )
            });
            drop(owner);
            if bound {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn publication_conversation_limit(&self) -> u64 {
        self.config.max_semantic_conversations_per_connection
    }

    fn ready_connection_incarnations(
        &self,
        conversation_id: ConversationId,
    ) -> Result<Vec<ConnectionIncarnation>, ParticipantSemanticError> {
        self.ensure_service_live()?;
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
        self.ensure_service_live()?;
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
        let Some(dispatch_state) = authority.obligation_debt_dispatch() else {
            return Err(publication_owner_missing(conversation_id));
        };
        let offered_through = offered
            .filter(|progress| progress.binding_epoch == binding_epoch)
            .map_or_else(
                || outbox.durable_ack_through(participant_id),
                |progress| progress.through_seq,
            );
        let decision = decide_obligation_debt_dispatch(
            dispatch_state,
            participant_id,
            binding_epoch,
            offered_through,
            |participant_id, binding_epoch, dispatch_after| {
                outbox
                    .delivery_after(participant_id, dispatch_after)
                    .map(|delivery| ParticipantPublication {
                        participant_id,
                        binding_epoch,
                        delivery,
                    })
            },
        );
        let publication = match decision {
            ObligationDebtDispatchDecision::Permit(publication) => publication,
            ObligationDebtDispatchDecision::Defer(_) => None,
            ObligationDebtDispatchDecision::Invariant(invariant) => {
                return Err(ParticipantSemanticError::Internal {
                    message: format!("obligation-debt dispatch invariant: {invariant:?}"),
                });
            }
        };
        drop(owner);
        Ok(publication)
    }

    fn publication_binding_is_current(
        &self,
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        binding_epoch: BindingEpoch,
    ) -> Result<bool, ParticipantSemanticError> {
        self.ensure_service_live()?;
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
        self.ensure_service_live()?;
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
        self.ensure_service_live()?;
        self.apply_observer_recovery(context, conversations, &request, target.as_ref())
    }

    fn handle_with_impact(
        &self,
        context: ParticipantConnectionContext,
        conversations: &mut ParticipantConnectionConversations,
        request: ClientRequest,
    ) -> ParticipantSemanticOutcome<ServerValue> {
        self.handle_request_with_impact(context, conversations, request)
    }

    fn handle(
        &self,
        context: ParticipantConnectionContext,
        conversations: &mut ParticipantConnectionConversations,
        request: ClientRequest,
    ) -> Result<ServerValue, ParticipantSemanticError> {
        self.handle_request_with_impact(context, conversations, request)
            .into_result()
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
