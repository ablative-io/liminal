//! The committed arm of both ack operations, plus the echo envelopes it mints.
//!
//! The seam is the frozen register's stage boundary, not the wire operation: on
//! this side of it a decision has already been SELECTED and what remains is the
//! same five-step commit either way — run the stage-6 connection-conversation
//! capacity gate, transition the frontier, put the durable row down, move the
//! member cursor with its sealed binding-fate token, project observer progress.
//! `ack_commit` additionally serves replay through `live: None`, which is why
//! the core sits here rather than beside the live arm that calls it.

use liminal::durability::bridge::block_on;

use liminal_protocol::lifecycle::{
    BindingState, MarkerAckCommit, ParticipantAckDecision, PresentedIdentity,
    RecipientAckObligations, SemanticConnectionCapacityDecision, apply_marker_ack_frontier,
    apply_participant_ack_frontier, apply_participant_ack_with_obligations,
};
use liminal_protocol::wire::{
    MarkerAck, MarkerAckEnvelope, MarkerAckResponse, ParticipantAck, ParticipantAckEnvelope,
    ParticipantAckResponse, ServerDiscriminant,
};

use super::super::barrier::{ArmOutcome, OperationFacts};
use super::super::facts::Digest;
use super::super::log::{StoredBindingEpoch, StoredOperation};
use super::super::observer_progress::ObserverProgressSourceMetadata;
use super::super::outbox_log::{OutboxLog, OutboxRow, StoredMarkerAckCommitted};
use super::super::state::{ConversationAuthority, DurableAppend, StateError};

use super::binding_fate::{progress_pending_binding_fate, progress_pending_marker_binding_fate};

impl ConversationAuthority {
    /// Shared zero-debt ack core: total selection plus the committed arm.
    ///
    /// Live mode carries the operation facts for the frozen stage-6
    /// connection-conversation capacity gate and appends the entry (advancing
    /// the log head) only for a capacity-admitted committed decision; replay
    /// mode (`live: None`) reproduces the durable classification without any
    /// connection-scoped gating, because the connection facts of the original
    /// commit are not durable classification inputs.
    pub(super) fn ack_commit(
        &mut self,
        request: &ParticipantAck,
        receiving_epoch: StoredBindingEpoch,
        obligations: &RecipientAckObligations,
        contiguously_available_through: u64,
        live: Option<(&OperationFacts, &dyn DurableAppend)>,
    ) -> Result<ArmOutcome, StateError> {
        let source_sequence = self.next_log_sequence;
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
        let decision = apply_participant_ack_with_obligations(
            identity,
            binding,
            receiving,
            request,
            obligations,
        )
        .map_err(|error| {
            StateError::invariant(format!(
                "participant ack obligation testimony disagrees with protocol state: {error:?}"
            ))
        })?;
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
                let observer_projection = commit.observer_progress_projection();
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
                progress_pending_binding_fate(slot, &commit)?;
                let outcome = commit.apply_to(&mut slot.member).map_err(|error| {
                    StateError::invariant(format!("ack cursor commit rejected: {error:?}"))
                })?;
                self.install_frontier(frontier_owner)?;
                let metadata = participant_ack_metadata(source_sequence, request);
                self.record_observer_progress_projection(observer_projection, metadata)?;
                Ok(ArmOutcome {
                    value: ParticipantAckResponse::ack_committed(outcome).into_server_value(),
                    newly_tracked,
                })
            }
        }
    }

    pub(super) fn commit_marker_ack(
        &mut self,
        request: &MarkerAck,
        operation_facts: &OperationFacts,
        outbox_log: &OutboxLog,
        commit: MarkerAckCommit,
    ) -> Result<ArmOutcome, StateError> {
        let capacity = match operation_facts.semantic_connection_capacity() {
            SemanticConnectionCapacityDecision::Commit(value) => value,
            SemanticConnectionCapacityDecision::Respond { limit } => {
                return Ok(ArmOutcome::respond(
                    MarkerAckResponse::connection_conversation_capacity_exceeded(
                        marker_ack_envelope(request),
                        limit,
                    )
                    .into_server_value(),
                ));
            }
        };
        let newly_tracked = capacity.newly_tracked();
        let observer_projection = commit.observer_progress_projection();
        let transitioned =
            apply_marker_ack_frontier(self.take_frontier()?, commit).map_err(|failure| {
                StateError::invariant(format!(
                    "marker ack frontier transition failed: {:?}",
                    failure.error()
                ))
            })?;
        let (commit, frontier) = transitioned.into_parts();
        let extension_sequence = self
            .outbox
            .as_ref()
            .ok_or_else(|| StateError::invariant("marker ack outbox owner is absent"))?
            .next_extension_sequence();
        let stored = StoredMarkerAckCommitted {
            request: commit.canonical_request(),
            receiving_binding_epoch: commit.receiving_binding_epoch(),
            offered_marker_delivery_seq: commit.offered_marker_delivery_seq(),
            delivered_binding_epoch: commit.delivered_binding_epoch(),
            from_cursor: commit.from_cursor(),
            resulting_cursor: commit.resulting_cursor(),
            base_log_head: self.next_log_sequence,
            extension_sequence,
        };
        let metadata = ObserverProgressSourceMetadata::marker_ack(
            stored.base_log_head,
            stored.extension_sequence,
            stored.request.conversation_id,
            stored.request.participant_id,
            stored.request.marker_delivery_seq,
            stored.resulting_cursor,
        );
        let row = OutboxRow::MarkerAckCommitted(stored);
        block_on(outbox_log.append(&row, extension_sequence))??;
        self.outbox
            .as_mut()
            .ok_or_else(|| StateError::invariant("marker ack outbox owner disappeared"))?
            .apply_row(extension_sequence, row)?;
        let slot = self.slots.get_mut(&request.participant_id).ok_or_else(|| {
            StateError::invariant("committed marker ack lost its participant slot")
        })?;
        // SITE ONE of `#26`. The sealed token MUST move with the member cursor,
        // and it must move BEFORE `apply_to` consumes the commit. Omitting this
        // is what left the token frozen behind an advanced member and had the
        // next ordinary ack refused at the invariant below.
        progress_pending_marker_binding_fate(slot, &commit)?;
        let outcome = commit.apply_to(&mut slot.member).map_err(|error| {
            StateError::invariant(format!("marker ack cursor commit rejected: {error:?}"))
        })?;
        self.install_frontier(frontier)?;
        self.offered_markers
            .remove(&(request.participant_id, request.marker_delivery_seq));
        self.record_observer_progress_projection(observer_projection, metadata)?;
        Ok(ArmOutcome {
            value: MarkerAckResponse::marker_ack_committed(outcome).into_server_value(),
            newly_tracked,
        })
    }
}

const fn participant_ack_metadata(
    source_sequence: u64,
    request: &ParticipantAck,
) -> ObserverProgressSourceMetadata {
    ObserverProgressSourceMetadata::participant_ack(
        source_sequence,
        request.conversation_id,
        request.participant_id,
        request.through_seq,
    )
}

const fn participant_ack_envelope(request: &ParticipantAck) -> ParticipantAckEnvelope {
    ParticipantAckEnvelope {
        conversation_id: request.conversation_id,
        participant_id: request.participant_id,
        capability_generation: request.capability_generation,
        through_seq: request.through_seq,
    }
}

/// Builds the echo envelope of one marker acknowledgement.
pub(super) const fn marker_ack_envelope(request: &MarkerAck) -> MarkerAckEnvelope {
    MarkerAckEnvelope {
        conversation_id: request.conversation_id,
        participant_id: request.participant_id,
        capability_generation: request.capability_generation,
        marker_delivery_seq: request.marker_delivery_seq,
    }
}
