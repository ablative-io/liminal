//! Cumulative-ack and marker-ack arms.
//!
//! Same discipline and error contract as the sibling operation arms: the
//! crate's total ack selectors classify and commit, the frozen stage-6
//! connection-conversation capacity gate runs at its register position, and
//! request-bound response authorities carry every reply.
//!
//! This file is the LIVE arm of both operations: it reads present state to
//! assemble each selector's classification inputs and it owns the typed impact
//! accounting, which is what makes it the entry point every dispatcher calls.
//! Three submodules carry the rest, split on seams the code already draws:
//! [`commit`] holds the committed arm shared by live and replay, [`replay`]
//! holds the cold-restore drivers that reconstruct a classification from a
//! durable row, and [`binding_fate`] holds the `#26` sealed-token pair whose
//! correctness argument is the contrast between its two members.

mod binding_fate;
mod commit;
mod replay;

use liminal_protocol::lifecycle::{
    BindingState, MarkerAckDecision, MarkerProofState, PresentedIdentity, RetainedCausalRecordKind,
    SemanticConnectionCapacityDecision, apply_marker_ack,
};
use liminal_protocol::wire::{
    BindingEpoch, DeliverySeq, MarkerAck, MarkerAckResponse, ParticipantAck, ParticipantId,
    ServerDiscriminant, ServerValue,
};

use crate::server::participant::dispatch_impact::DispatchImpactAccumulator;

use super::barrier::{ArmOutcome, OperationFacts};
use super::facts::Digest;
use super::marker_progress::marker_delivery_progress;
use super::outbox_log::OutboxLog;
use super::state::{ConversationAuthority, DurableAppend, StateError};

use commit::marker_ack_envelope;

impl ConversationAuthority {
    #[cfg(test)]
    pub(super) fn apply_ack(
        &mut self,
        request: &ParticipantAck,
        operation_facts: &OperationFacts,
        appender: &dyn DurableAppend,
    ) -> Result<ArmOutcome, StateError> {
        let mut impact = DispatchImpactAccumulator::new();
        self.apply_ack_with_impact(request, operation_facts, appender, &mut impact)
    }

    /// Applies one cumulative acknowledgement over the zero-debt selector.
    pub(super) fn apply_ack_with_impact(
        &mut self,
        request: &ParticipantAck,
        operation_facts: &OperationFacts,
        appender: &dyn DurableAppend,
        impact: &mut DispatchImpactAccumulator,
    ) -> Result<ArmOutcome, StateError> {
        if self
            .obligation_debt_dispatch()
            .is_some_and(|state| state.episode().is_some())
        {
            return self.apply_nonzero_ack_with_impact(request, operation_facts, appender, impact);
        }
        let receiving_epoch = BindingEpoch::new(
            operation_facts.receiving_incarnation,
            request.capability_generation,
        );
        let outbox = self
            .outbox
            .as_ref()
            .ok_or_else(|| StateError::invariant("participant ack outbox owner is absent"))?;
        let acknowledged_through = self.slots.get(&request.participant_id).map_or_else(
            || outbox.durable_ack_through(request.participant_id),
            |slot| slot.member.cursor(),
        );
        let (obligations, contiguously_available_through) =
            outbox.recipient_ack_obligations(request.participant_id, acknowledged_through)?;
        let outcome = self.ack_commit(
            request,
            receiving_epoch.into(),
            &obligations,
            contiguously_available_through,
            Some((operation_facts, appender)),
        )?;
        if matches!(outcome.value, ServerValue::AckCommitted(_)) {
            self.record_acknowledged(request.participant_id, impact);
            self.record_episode_changed(impact);
        }
        Ok(outcome)
    }

    /// Applies one marker acknowledgement over the zero-debt marker selector.
    pub(super) fn apply_marker_ack_with_impact(
        &mut self,
        request: &MarkerAck,
        operation_facts: &OperationFacts,
        outbox_log: &OutboxLog,
        impact: &mut DispatchImpactAccumulator,
    ) -> Result<ArmOutcome, StateError> {
        let owed = self
            .obligation_debt_dispatch()
            .is_some_and(|state| state.episode().is_some());
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
        let offered = self
            .offered_markers
            .get(&(request.participant_id, request.marker_delivery_seq))
            .filter(|binding_epoch| **binding_epoch == receiving_epoch)
            .map(|binding_epoch| (request.marker_delivery_seq, *binding_epoch));
        let (expected_marker, delivered_binding_epoch, progress) = match offered {
            Some((delivery_seq, binding_epoch)) => (
                Some(delivery_seq),
                binding_epoch,
                Some(marker_delivery_progress(
                    self,
                    request.participant_id,
                    binding_epoch,
                    delivery_seq,
                )?),
            ),
            None => (None, receiving_epoch, None),
        };
        // The server's half of the frozen selector's AckNoOp arm: a retained
        // compaction marker of THIS participant sitting exactly AT the cursor
        // was accepted — the cursor can only reach its own marker's sequence
        // by a marker-ack or by an ordinary ack crossing it (which retires the
        // anchor). A client that still owes the marker-ack (its resume state
        // predates the crossing) re-presents it here, and without this flag
        // the selector answers MarkerMismatch for an acknowledgement that is
        // merely redundant — the 2026-08-07 kernel death.
        let accepted_marker_at_cursor =
            self.marker_record_accepted_at_cursor(request.participant_id, cursor);
        let marker_state = MarkerProofState::new(
            cursor,
            accepted_marker_at_cursor,
            expected_marker,
            delivered_binding_epoch,
            progress,
        );
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
            MarkerAckDecision::Commit(commit) => {
                let outcome =
                    self.commit_marker_ack(request, operation_facts, outbox_log, commit)?;
                if matches!(outcome.value, ServerValue::MarkerAckCommitted(_)) {
                    self.record_acknowledged(request.participant_id, impact);
                    if owed {
                        self.record_episode_changed(impact);
                    }
                }
                Ok(outcome)
            }
        }
    }

    /// Answers whether a retained compaction marker owned by this participant
    /// sits exactly at the given cursor — the durable fact behind the frozen
    /// selector's `accepted_marker_at_cursor` flag. The retained-record census
    /// survives restarts, so a marker accepted before a boot is still visible
    /// here when its offer entry is not.
    ///
    /// Shared with the credential-attach marker-proof site
    /// (`ops_attach_lookup::attach_marker_proof_state`) under board #12: two
    /// sites feed one frozen selector the same field, so they derive it from
    /// one durable fact rather than each answering for itself.
    pub(super) fn marker_record_accepted_at_cursor(
        &self,
        participant_id: ParticipantId,
        cursor: DeliverySeq,
    ) -> bool {
        self.frontier().is_some_and(|owner| {
            owner
                .frontiers()
                .retained_marker_records()
                .iter()
                .any(|record| {
                    record.delivery_seq == cursor
                        && matches!(
                            record.kind,
                            RetainedCausalRecordKind::CompactionMarker { participant_index, .. }
                                if participant_index == participant_id
                        )
                })
        })
    }
}
