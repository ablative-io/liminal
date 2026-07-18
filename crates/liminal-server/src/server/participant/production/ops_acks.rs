//! Cumulative-ack and marker-ack arms.
//!
//! Same discipline and error contract as the sibling operation arms: the
//! crate's total ack selectors classify and commit, the frozen stage-6
//! connection-conversation capacity gate runs at its register position, and
//! request-bound response authorities carry every reply.

use liminal_protocol::lifecycle::{
    BindingState, MarkerAckDecision, MarkerProofState, ParticipantAckDecision, PresentedIdentity,
    SemanticConnectionCapacityDecision, apply_marker_ack, apply_participant_ack,
    apply_participant_ack_frontier,
};
use liminal_protocol::wire::{
    BindingEpoch, MarkerAck, MarkerAckEnvelope, MarkerAckResponse, ParticipantAck,
    ParticipantAckEnvelope, ParticipantAckResponse, ServerDiscriminant, ServerValue,
};

use super::barrier::{ArmOutcome, OperationFacts};
use super::facts::Digest;
use super::log::{StoredAck, StoredBindingEpoch, StoredOperation};
use super::state::{ConversationAuthority, DurableAppend, StateError};

impl ConversationAuthority {
    /// Applies one cumulative acknowledgement over the zero-debt selector.
    pub(super) fn apply_ack(
        &mut self,
        request: &ParticipantAck,
        operation_facts: &OperationFacts,
        appender: &dyn DurableAppend,
    ) -> Result<ArmOutcome, StateError> {
        let receiving_epoch = BindingEpoch::new(
            operation_facts.receiving_incarnation,
            request.capability_generation,
        );
        let contiguous = self.contiguously_available_through();
        self.ack_commit(
            request,
            receiving_epoch.into(),
            contiguous,
            Some((operation_facts, appender)),
        )
    }

    /// Replays one committed zero-debt ack entry from its stored inputs.
    pub(super) fn replay_zero_debt_ack(
        &mut self,
        request: StoredAck,
        receiving_epoch: StoredBindingEpoch,
        contiguously_available_through: u64,
    ) -> Result<(), StateError> {
        let request = request.to_request()?;
        let outcome = self.ack_commit(
            &request,
            receiving_epoch,
            contiguously_available_through,
            None,
        )?;
        // A durable ack entry is appended only for a committed decision, so a
        // replay that classifies as anything else diverged from history.
        if !matches!(outcome.value, ServerValue::AckCommitted(_)) {
            return Err(StateError::invariant(
                "durable zero-debt ack entry replayed to a non-committed decision",
            ));
        }
        self.advance_log_head()?;
        Ok(())
    }

    /// Shared zero-debt ack core: total selection plus the committed arm.
    ///
    /// Live mode carries the operation facts for the frozen stage-6
    /// connection-conversation capacity gate and appends the entry (advancing
    /// the log head) only for a capacity-admitted committed decision; replay
    /// mode (`live: None`) reproduces the durable classification without any
    /// connection-scoped gating, because the connection facts of the original
    /// commit are not durable classification inputs.
    fn ack_commit(
        &mut self,
        request: &ParticipantAck,
        receiving_epoch: StoredBindingEpoch,
        contiguously_available_through: u64,
        live: Option<(&OperationFacts, &dyn DurableAppend)>,
    ) -> Result<ArmOutcome, StateError> {
        let receiving = receiving_epoch.to_epoch()?;
        let identity = self
            .slots
            .get(&request.participant_id)
            .map_or(PresentedIdentity::Absent, |slot| {
                PresentedIdentity::<Digest, Digest, Digest>::Live(&slot.member)
            });
        let binding_detached = BindingState::Detached;
        let binding = self
            .slots
            .get(&request.participant_id)
            .map_or(&binding_detached, |slot| &slot.binding);
        let decision = apply_participant_ack(
            identity,
            binding,
            receiving,
            request,
            contiguously_available_through,
        );
        match decision {
            ParticipantAckDecision::Respond(response) => {
                // The crate's total ack selector conflates the frozen stages:
                // its lookup rows (2-5) precede stage-6 capacity, while its
                // continuity rows (stage 7) follow it. The split below is a
                // TRANSCRIPTION of the register's stage numbers over the
                // typed discriminants — no lifecycle rule is re-derived.
                let stage_seven = matches!(
                    response.discriminant(),
                    ServerDiscriminant::AckNoOp
                        | ServerDiscriminant::AckGap
                        | ServerDiscriminant::AckRegression
                );
                if stage_seven {
                    if let Some((operation_facts, _)) = live {
                        if let SemanticConnectionCapacityDecision::Respond { limit } =
                            operation_facts.semantic_connection_capacity()
                        {
                            return Ok(ArmOutcome::respond(
                                ParticipantAckResponse::connection_conversation_capacity_exceeded(
                                    participant_ack_envelope(request),
                                    limit,
                                )
                                .into_server_value(),
                            ));
                        }
                    }
                }
                Ok(ArmOutcome::respond(response.into_server_value()))
            }
            ParticipantAckDecision::Commit(commit) => {
                let transitioned = apply_participant_ack_frontier(self.take_frontier()?, commit)
                    .map_err(|failure| {
                        StateError::invariant(format!(
                            "participant ack frontier transition failed: {:?}",
                            failure.error()
                        ))
                    })?;
                let (commit, frontier_owner) = transitioned.into_parts();
                let mut newly_tracked = false;
                if let Some((operation_facts, appender)) = live {
                    // Stage 6 precedes the stage-13 commit: an untracked
                    // conversation over a full connection map refuses before
                    // anything durable or cursor-visible happens (the unused
                    // commit decision is pure state that is simply not
                    // applied).
                    let capacity = match operation_facts.semantic_connection_capacity() {
                        SemanticConnectionCapacityDecision::Commit(value) => value,
                        SemanticConnectionCapacityDecision::Respond { limit } => {
                            return Ok(ArmOutcome::respond(
                                ParticipantAckResponse::connection_conversation_capacity_exceeded(
                                    participant_ack_envelope(request),
                                    limit,
                                )
                                .into_server_value(),
                            ));
                        }
                    };
                    newly_tracked = capacity.newly_tracked();
                    let operation = StoredOperation::ZeroDebtAck {
                        request: request.into(),
                        receiving_epoch,
                        contiguously_available_through,
                    };
                    appender.append(&operation, self.next_log_sequence)?;
                    self.advance_log_head()?;
                }
                let slot = self.slots.get_mut(&request.participant_id).ok_or_else(|| {
                    StateError::invariant("committed ack lost its participant slot")
                })?;
                let outcome = commit.apply_to(&mut slot.member).map_err(|error| {
                    StateError::invariant(format!("ack cursor commit rejected: {error:?}"))
                })?;
                self.install_frontier(frontier_owner);
                Ok(ArmOutcome {
                    value: ParticipantAckResponse::ack_committed(outcome).into_server_value(),
                    newly_tracked,
                })
            }
        }
    }

    /// Applies one marker acknowledgement over the zero-debt marker selector.
    ///
    /// This binding delivers no markers (no delivery pump exists for
    /// participant records yet), so the durable marker facts are factually
    /// empty: no expected marker, no delivery witness. The crate selects the
    /// refusal; a committed marker ack is unreachable until delivery exists.
    pub(super) fn apply_marker_ack(
        &self,
        request: &MarkerAck,
        operation_facts: &OperationFacts,
    ) -> Result<ArmOutcome, StateError> {
        let receiving_epoch = BindingEpoch::new(
            operation_facts.receiving_incarnation,
            request.capability_generation,
        );
        let identity = self
            .slots
            .get(&request.participant_id)
            .map_or(PresentedIdentity::Absent, |slot| {
                PresentedIdentity::<Digest, Digest, Digest>::Live(&slot.member)
            });
        let binding_detached = BindingState::Detached;
        let binding = self
            .slots
            .get(&request.participant_id)
            .map_or(&binding_detached, |slot| &slot.binding);
        let cursor = self
            .slots
            .get(&request.participant_id)
            .map_or(0, |slot| slot.member.cursor());
        let marker_state = MarkerProofState::new(cursor, false, None, receiving_epoch, None);
        match apply_marker_ack(identity, binding, receiving_epoch, request, &marker_state) {
            MarkerAckDecision::Respond(response) => {
                // Same frozen-stage transcription as the normal-ack arm: the
                // selector's lookup rows (2-5) precede stage-6 capacity; its
                // marker-proof rows (stage 7) follow it.
                let stage_seven = matches!(
                    response.discriminant(),
                    ServerDiscriminant::AckNoOp
                        | ServerDiscriminant::MarkerNotDelivered
                        | ServerDiscriminant::MarkerMismatch
                );
                if stage_seven {
                    if let SemanticConnectionCapacityDecision::Respond { limit } =
                        operation_facts.semantic_connection_capacity()
                    {
                        return Ok(ArmOutcome::respond(
                            MarkerAckResponse::connection_conversation_capacity_exceeded(
                                marker_ack_envelope(request),
                                limit,
                            )
                            .into_server_value(),
                        ));
                    }
                }
                Ok(ArmOutcome::respond(response.into_server_value()))
            }
            MarkerAckDecision::Commit(_) => Err(StateError::invariant(
                "marker ack committed although no marker was ever delivered",
            )),
        }
    }
}

/// Builds the echo envelope of one cumulative acknowledgement.
const fn participant_ack_envelope(request: &ParticipantAck) -> ParticipantAckEnvelope {
    ParticipantAckEnvelope {
        conversation_id: request.conversation_id,
        participant_id: request.participant_id,
        capability_generation: request.capability_generation,
        through_seq: request.through_seq,
    }
}

/// Builds the echo envelope of one marker acknowledgement.
const fn marker_ack_envelope(request: &MarkerAck) -> MarkerAckEnvelope {
    MarkerAckEnvelope {
        conversation_id: request.conversation_id,
        participant_id: request.participant_id,
        capability_generation: request.capability_generation,
        marker_delivery_seq: request.marker_delivery_seq,
    }
}
