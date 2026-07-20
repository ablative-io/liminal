//! Detach arm plus the cold replay driver (ack arms live in
//! [`super::ops_acks`]).
//!
//! Same discipline and error contract as [`super::ops_enroll`] and
//! [`super::ops_attach`]: shared lookups
//! classify, crate transitions commit, the A3 aggregate barrier orders every
//! shell event behind its durable append, and request-bound response
//! authorities carry every reply.

use liminal_protocol::lifecycle::{
    AggregateOperationDecision, CommittedBindingTerminalPosition, DetachCell, DetachLookupContext,
    DetachLookupResult, DetachTokenResolution, PresentedIdentity, ResolvedIdentity,
    RetainedRecordCharge, SemanticConnectionCapacityDecision, apply_detach_frontier, commit_detach,
    decide_detached_operation, lookup_detach,
};
use liminal_protocol::wire::{BindingEpoch, DetachEnvelope, DetachRequest, DetachResponse};

use super::barrier::{ArmOutcome, CommitMode, OperationFacts, commit_through_barrier};
use super::facts::{self, Digest};
use super::frontier;
use super::log::{
    StoredBindingEpoch, StoredDetachRequest, StoredDetached, StoredDetachedCause,
    StoredDetachedSource, StoredOperation, StoredTerminalDisposition,
};
use super::observer_progress::ObserverProgressSourceMetadata;
use super::state::{ConversationAuthority, DurableAppend, StateError};

impl ConversationAuthority {
    /// Applies one explicit detach request end to end.
    pub(super) fn apply_detach(
        &mut self,
        request: &DetachRequest,
        operation_facts: &OperationFacts,
        appender: &dyn DurableAppend,
    ) -> Result<ArmOutcome, StateError> {
        let envelope = detach_envelope(request);
        let receiving_epoch = BindingEpoch::new(
            operation_facts.receiving_incarnation,
            request.capability_generation,
        );
        let verifier = facts::detach_request_verifier(request);
        let Some(slot) = self.slots.get(&request.participant_id) else {
            return Ok(ArmOutcome::respond(
                DetachResponse::participant_unknown(envelope).into_server_value(),
            ));
        };
        let token_resolution = if slot.exact_detach_token == Some(request.detach_attempt_token) {
            DetachTokenResolution::Exact(ResolvedIdentity::<Digest, Digest, Digest>::Live(
                &slot.member,
            ))
        } else {
            DetachTokenResolution::NoExactMatch
        };
        let lookup = lookup_detach(&DetachLookupContext {
            token_resolution,
            presented_identity: PresentedIdentity::<Digest, Digest, Digest>::Live(&slot.member),
            cell: &slot.cell,
            binding: &slot.binding,
            receiving_binding_epoch: Some(receiving_epoch),
            request,
            request_verifier: verifier,
            observer_progress: self.observer_progress,
        });
        match lookup {
            DetachLookupResult::Authorized { .. } => {}
            DetachLookupResult::ParticipantUnknown(_) => {
                return Ok(ArmOutcome::respond(
                    DetachResponse::participant_unknown(envelope).into_server_value(),
                ));
            }
            DetachLookupResult::NoBinding(_) => {
                return Ok(ArmOutcome::respond(
                    DetachResponse::no_binding(envelope).into_server_value(),
                ));
            }
            DetachLookupResult::StaleAuthority(value) => {
                return Ok(ArmOutcome::respond(
                    DetachResponse::stale_authority(value).into_server_value(),
                ));
            }
            DetachLookupResult::DetachInProgress(value) => {
                return Ok(ArmOutcome::respond(
                    DetachResponse::detach_in_progress(value).into_server_value(),
                ));
            }
            DetachLookupResult::DetachCommitted(value) => {
                return Ok(ArmOutcome::respond(
                    DetachResponse::detach_committed(value).into_server_value(),
                ));
            }
            DetachLookupResult::Retired(_) => {
                return Err(StateError::invariant(
                    "retired identity observed in a binding that mints no tombstones",
                ));
            }
            DetachLookupResult::PendingReplayRequired(_) => {
                return Err(StateError::invariant(
                    "pending detach cell observed in a binding that commits detaches immediately",
                ));
            }
        }
        // Stage 6: connection-conversation capacity (register row 5641) —
        // after the lookup stages, before the committing transaction.
        let capacity = match operation_facts.semantic_connection_capacity() {
            SemanticConnectionCapacityDecision::Commit(value) => value,
            SemanticConnectionCapacityDecision::Respond { limit } => {
                return Ok(ArmOutcome::respond(
                    DetachResponse::connection_conversation_capacity_exceeded(envelope, limit)
                        .into_server_value(),
                ));
            }
        };
        let (terminal_order, terminal_seq) = self.allocate_position()?;
        let outcome = self.detach_commit(
            request,
            verifier,
            DetachCommitPosition {
                receiving_epoch: receiving_epoch.into(),
                terminal_order,
                terminal_seq,
            },
            CommitMode::Live(appender),
        )?;
        Ok(ArmOutcome::committed(
            DetachResponse::detach_committed(outcome).into_server_value(),
            capacity,
        ))
    }

    /// Replays one committed detach entry from its stored inputs.
    pub(super) fn replay_detached(
        &mut self,
        inputs: DetachReplayInputs,
        stored_event: &[u8],
        sequence: u64,
    ) -> Result<(), StateError> {
        let request = inputs.request.to_request()?;
        self.detach_commit(
            &request,
            inputs.verifier,
            DetachCommitPosition {
                receiving_epoch: inputs.receiving_epoch,
                terminal_order: inputs.terminal_order,
                terminal_seq: inputs.terminal_seq,
            },
            CommitMode::Replay {
                stored_event,
                sequence,
            },
        )?;
        Ok(())
    }

    /// Shared immediate-detach commit core (live and replay paths).
    ///
    /// Detach is ONE event: the consumed transition carries the terminal
    /// append, floor transition, cell replacement, and binding release as one
    /// non-decomposable value through the A3 barrier.
    fn detach_commit(
        &mut self,
        request: &DetachRequest,
        verifier: Digest,
        position: DetachCommitPosition,
        mode: CommitMode<'_>,
    ) -> Result<liminal_protocol::wire::DetachCommitted, StateError> {
        let DetachCommitPosition {
            receiving_epoch,
            terminal_order,
            terminal_seq,
        } = position;
        let source_sequence = self.next_log_sequence;
        let (participant_id, mut slot) = self
            .slots
            .remove_entry(&request.participant_id)
            .ok_or_else(|| {
                StateError::invariant("detach commit requires an enrolled participant slot")
            })?;
        let receiving = receiving_epoch.to_epoch()?;
        let binding = {
            let lookup = lookup_detach(&DetachLookupContext {
                token_resolution: DetachTokenResolution::<Digest, Digest, Digest>::NoExactMatch,
                presented_identity: PresentedIdentity::<Digest, Digest, Digest>::Live(&slot.member),
                cell: &slot.cell,
                binding: &slot.binding,
                receiving_binding_epoch: Some(receiving),
                request,
                request_verifier: verifier,
                observer_progress: self.observer_progress,
            });
            let DetachLookupResult::Authorized { binding, .. } = lookup else {
                return Err(StateError::invariant(
                    "detach commit inputs were not authorized by the shared lookup",
                ));
            };
            binding
        };
        let verified_request = binding
            .verify_detach_request(request.clone(), verifier)
            .map_err(|error| {
                StateError::invariant(format!("protocol detach verification failed: {error:?}"))
            })?;
        let committed = commit_detach(
            slot.member,
            verified_request,
            slot.cell,
            CommittedBindingTerminalPosition::new(terminal_order, terminal_seq),
        )
        .map_err(|error| {
            StateError::invariant(format!("protocol detach transition failed: {error:?}"))
        })?;
        let observer_projection = committed.observer_progress_projection();
        let terminal = committed.terminal();
        let encoded_charge = frontier::terminal_charge(
            terminal.conversation_id(),
            terminal.participant_id(),
            terminal.binding_epoch(),
            terminal.admission_order().transaction_order(),
            terminal.delivery_seq(),
        )?;
        let charge = RetainedRecordCharge::new(
            terminal.delivery_seq(),
            terminal.admission_order(),
            encoded_charge,
        );
        let transitioned = apply_detach_frontier(self.take_frontier()?, committed, charge)
            .map_err(|failure| {
                StateError::invariant(format!(
                    "detach frontier transition failed: {:?}",
                    failure.error()
                ))
            })?;
        let (committed, frontier_owner) = transitioned.into_parts();
        let shell = self.take_shell()?;
        let barrier = match decide_detached_operation(shell, committed) {
            Ok(AggregateOperationDecision::Commit(barrier)) => barrier,
            Ok(AggregateOperationDecision::Refused(refusal)) => {
                return Err(StateError::ShellRefused {
                    reason: refusal.reason(),
                });
            }
            Err(fault) => {
                return Err(StateError::invariant(format!(
                    "detach event pairing fault: {:?}",
                    fault.reason()
                )));
            }
        };
        let make_operation =
            |event: Vec<u8>| stored_detached_operation(request, verifier, position, event);
        let (shell, committed) =
            commit_through_barrier(barrier, mode, self.next_log_sequence, &make_operation)?;
        self.shell = Some(shell);
        self.install_frontier(frontier_owner);
        self.advance_log_head()?;
        let (member, _terminal, binding_state, cell, outcome) = committed.into_parts();
        slot.member = member;
        slot.binding = binding_state;
        slot.cell = DetachCell::Committed(cell);
        slot.exact_detach_token = Some(request.detach_attempt_token);
        self.slots.insert(participant_id, slot);
        let metadata = detach_metadata(source_sequence, request, terminal_seq);
        self.record_observer_progress_projection(observer_projection, metadata)?;
        self.observe_replayed_position(terminal_order, terminal_seq)?;
        Ok(outcome)
    }
}

#[derive(Clone, Copy)]
struct DetachCommitPosition {
    receiving_epoch: StoredBindingEpoch,
    terminal_order: u64,
    terminal_seq: u64,
}

fn stored_detached_operation(
    request: &DetachRequest,
    verifier: Digest,
    position: DetachCommitPosition,
    event: Vec<u8>,
) -> StoredOperation {
    StoredOperation::Detached {
        row: StoredDetached {
            participant_id: request.participant_id,
            binding_epoch: position.receiving_epoch,
            cause: StoredDetachedCause::CleanDeregister,
            terminal_order: position.terminal_order,
            disposition: StoredTerminalDisposition::Committed {
                terminal_seq: position.terminal_seq,
            },
            source: StoredDetachedSource::ExplicitRequestCommitted {
                request: request.into(),
                secret_verified: true,
                verifier,
                receiving_epoch: position.receiving_epoch,
                event,
            },
        },
    }
}

const fn detach_metadata(
    source_sequence: u64,
    request: &DetachRequest,
    terminal_seq: u64,
) -> ObserverProgressSourceMetadata {
    ObserverProgressSourceMetadata::detached(
        source_sequence,
        request.conversation_id,
        request.participant_id,
        terminal_seq,
    )
}

/// Stored inputs of one committed detach entry, regrouped for replay.
#[derive(Clone, Copy)]
pub(super) struct DetachReplayInputs {
    /// Wire request inputs.
    pub(super) request: StoredDetachRequest,
    /// Canonical non-secret request verifier.
    pub(super) verifier: Digest,
    /// Binding epoch of the receiving connection.
    pub(super) receiving_epoch: StoredBindingEpoch,
    /// Assigned terminal transaction order.
    pub(super) terminal_order: u64,
    /// Assigned terminal delivery sequence.
    pub(super) terminal_seq: u64,
}

/// Builds the echo envelope of one detach request.
const fn detach_envelope(request: &DetachRequest) -> DetachEnvelope {
    DetachEnvelope {
        conversation_id: request.conversation_id,
        participant_id: request.participant_id,
        capability_generation: request.capability_generation,
        detach_attempt_token: request.detach_attempt_token,
    }
}
