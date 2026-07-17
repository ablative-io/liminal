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
    SemanticConnectionCapacityDecision, commit_detach, decide_detached_operation, lookup_detach,
};
use liminal_protocol::wire::{BindingEpoch, DetachEnvelope, DetachRequest, DetachResponse};

use super::barrier::{ArmOutcome, CommitMode, OperationFacts, commit_through_barrier};
use super::facts::{self, Digest};
use super::log::{OperationLog, StoredBindingEpoch, StoredDetachRequest, StoredOperation};
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
            receiving_epoch.into(),
            terminal_order,
            terminal_seq,
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
            inputs.receiving_epoch,
            inputs.terminal_order,
            inputs.terminal_seq,
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
    #[allow(clippy::too_many_arguments)]
    fn detach_commit(
        &mut self,
        request: &DetachRequest,
        verifier: Digest,
        receiving_epoch: StoredBindingEpoch,
        terminal_order: u64,
        terminal_seq: u64,
        mode: CommitMode<'_>,
    ) -> Result<liminal_protocol::wire::DetachCommitted, StateError> {
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
        let make_operation = |event: Vec<u8>| StoredOperation::Detached {
            request: request.into(),
            verifier,
            receiving_epoch,
            terminal_order,
            terminal_seq,
            event,
        };
        let (shell, committed) =
            commit_through_barrier(barrier, mode, self.next_log_sequence, &make_operation)?;
        self.shell = Some(shell);
        self.advance_log_head()?;
        let (member, _terminal, binding_state, cell, outcome) = committed.into_parts();
        slot.member = member;
        slot.binding = binding_state;
        slot.cell = DetachCell::Committed(cell);
        slot.exact_detach_token = Some(request.detach_attempt_token);
        self.slots.insert(participant_id, slot);
        self.next_order = self.next_order.max(terminal_order.saturating_add(1));
        self.next_seq = self.next_seq.max(terminal_seq.saturating_add(1));
        Ok(outcome)
    }

    /// Cold-replays one conversation's complete durable log.
    pub(super) async fn replay(
        conversation_id: u64,
        log: &OperationLog,
    ) -> Result<Self, StateError> {
        let mut authority = Self::empty(conversation_id);
        let mut sequence = 0_u64;
        loop {
            let page = log.read_page(sequence).await?;
            if page.is_empty() {
                break;
            }
            let page_len = page.len();
            for (stored_sequence, operation) in page {
                if stored_sequence != sequence {
                    return Err(StateError::Log(super::log::OperationLogError::Sequence {
                        expected: sequence,
                        actual: stored_sequence,
                    }));
                }
                authority.replay_operation(operation, stored_sequence)?;
                sequence = sequence
                    .checked_add(1)
                    .ok_or(StateError::AllocationExhausted {
                        domain: "log sequence",
                    })?;
            }
            if page_len < super::log::READ_BATCH_SIZE {
                break;
            }
        }
        Ok(authority)
    }

    /// Replays one durable entry through the exact live transition cores.
    fn replay_operation(
        &mut self,
        operation: StoredOperation,
        sequence: u64,
    ) -> Result<(), StateError> {
        match operation {
            StoredOperation::Genesis { event } => self.replay_genesis(&event),
            StoredOperation::Enrolled {
                request,
                allocation,
                event,
            } => self.replay_enrolled(request, &allocation, &event, sequence),
            StoredOperation::Attached {
                request,
                secret_verified,
                allocation,
                event,
            } => {
                if !secret_verified {
                    return Err(StateError::invariant(
                        "durable attach entry recorded an unverified secret",
                    ));
                }
                self.replay_attached(request, &allocation, &event, sequence)
            }
            StoredOperation::Detached {
                request,
                verifier,
                receiving_epoch,
                terminal_order,
                terminal_seq,
                event,
            } => self.replay_detached(
                DetachReplayInputs {
                    request,
                    verifier,
                    receiving_epoch,
                    terminal_order,
                    terminal_seq,
                },
                &event,
                sequence,
            ),
            StoredOperation::ZeroDebtAck {
                request,
                receiving_epoch,
                contiguously_available_through,
            } => {
                self.replay_zero_debt_ack(request, receiving_epoch, contiguously_available_through)
            }
        }
    }
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
