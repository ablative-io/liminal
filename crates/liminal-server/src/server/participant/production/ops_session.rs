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

use crate::server::participant::dispatch_impact::DispatchImpactAccumulator;

use super::barrier::{ArmOutcome, CommitMode, OperationFacts, commit_through_barrier};
use super::facts::{self, Digest};
use super::frontier;
use super::log::{
    StoredBindingEpoch, StoredDetachRequest, StoredDetached, StoredDetachedCause,
    StoredDetachedSource, StoredOperation, StoredTerminalDisposition,
};
use super::observer_progress::ObserverProgressSourceMetadata;
use super::outbox_projection::ReplayedProjectionFacts;
use super::presented_refusal::PresentedRefusal;
use super::state::{ConversationAuthority, DurableAppend, StateError};

/// Selects the lawful §0.16 answer for a detach blocked at the seam.
///
/// Condition 1 reuses the register's `ObserverBackpressure` row for detach
/// (register rows 5669, 5673); condition 2 mints the settlement row with the
/// epoch the frontier itself named. Answering condition 2 with
/// `ObserverBackpressure` is OUTLAWED — it would promise an
/// `ObserverProgressed` that nothing sends. Condition 3 (armed fenced recovery)
/// is EXCLUDED BY CENSUS (board #13) and `Unclassified` was never one of the
/// amendment's conditions; both keep the pre-amendment bare close.
const fn detach_settlement_answer(
    condition: liminal_protocol::lifecycle::PrecedenceCondition,
    envelope: DetachEnvelope,
    committed_binding_epoch: BindingEpoch,
    observer_progress: u64,
) -> Option<DetachResponse> {
    use liminal_protocol::lifecycle::PrecedenceCondition as C;
    match condition {
        C::BindingTerminal => Some(DetachResponse::observer_backpressure(
            envelope,
            committed_binding_epoch,
            liminal_protocol::wire::ObserverBackpressureState::initial(observer_progress),
        )),
        C::MarkerDrain { settlement_epoch } => Some(
            DetachResponse::marker_settlement_backpressure(&envelope, settlement_epoch),
        ),
        C::FencedRecovery | C::Unclassified => None,
    }
}

impl ConversationAuthority {
    /// Applies one explicit detach request end to end.
    #[expect(
        clippy::too_many_lines,
        reason = "the amendment adds the position-allocator capture and its restore arm; the \
                  capture must sit in the same function as the allocator it captures, or the \
                  pairing stops being checkable by reading"
    )]
    pub(super) fn apply_detach_with_impact(
        &mut self,
        request: &DetachRequest,
        operation_facts: &OperationFacts,
        appender: &dyn DurableAppend,
        impact: &mut DispatchImpactAccumulator,
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
        // §0.16 obligation 1: captured before the allocator runs, restored if
        // the frontier transition presents a settlement refusal.
        let captured_positions = self.position_allocators();
        let (terminal_order, terminal_seq) = self.allocate_position()?;
        let source_log_sequence = self.next_log_sequence;
        let position = DetachCommitPosition {
            receiving_epoch: receiving_epoch.into(),
            terminal_order,
            terminal_seq,
        };
        let outcome =
            match self.detach_commit(request, verifier, position, CommitMode::Live(appender)) {
                Ok(outcome) => outcome,
                Err(error) => {
                    if matches!(error, StateError::PresentedRefusal(_)) {
                        self.restore_position_allocators(captured_positions);
                    }
                    return Err(error);
                }
            };
        let source = stored_detached_operation(request, verifier, position, Vec::new());
        self.record_produced_source(
            source_log_sequence,
            &source,
            ReplayedProjectionFacts::none(),
            appender,
            impact,
        )?;
        self.record_binding_changed(request.participant_id, impact);
        self.record_episode_changed(impact);
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
    #[expect(
        clippy::too_many_lines,
        reason = "the amendment adds ONE conditional arm (the settlement refusal and its slot \
                  plus frontier restoration) to an already-long commit core; splitting it would \
                  move the take/install pairing across a function boundary, which is exactly the \
                  coupling the restoration obligation depends on being visible in one place"
    )]
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
        let presenting = matches!(mode, CommitMode::Live(_));
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
            // Cloned rather than moved: §0.16 obligation 1 needs the WHOLE slot
            // entry intact if the frontier transition below presents a
            // settlement refusal, and a partial move leaves nothing to restore.
            slot.member.clone(),
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
        let transitioned = match apply_detach_frontier(self.take_frontier()?, committed, charge) {
            Ok(transitioned) => transitioned,
            Err(failure) => {
                let error = failure.error();
                // Participant contract §0.16 condition 2, detach wrapper. Detach
                // requires an existing attached binding — a membership predicate
                // strictly stronger than attach's two — which is what entitles
                // this arm to the labelled row and its `0x0202 MarkerSettled`
                // wake. Condition 1 keeps the register's existing
                // `ObserverBackpressure` row; conditions 3 and Unclassified keep
                // the pre-amendment bare close (board #13 census tripwire).
                //
                // ⛔ REPLAY IS NEVER PRESENTED: a durable row that replays into
                // Precedence is a drifted log, not backpressure.
                let answer = match (presenting, error) {
                    (
                        true,
                        liminal_protocol::lifecycle::LiveFrontierError::Precedence(condition),
                    ) => detach_settlement_answer(
                        condition,
                        detach_envelope(request),
                        receiving,
                        self.observer_progress,
                    ),
                    _ => None,
                };
                let Some(response) = answer else {
                    return Err(StateError::invariant(format!(
                        "detach frontier transition failed: {error:?}"
                    )));
                };
                // §0.16 obligation 1: `detach_commit` consumed the slot entry and
                // the frontier before this refusal existed, and
                // `LiveFrontierFailure::into_parts` returns only the owner. Both
                // go back, and the arm puts the position allocators back, before
                // the refusal leaves this function.
                let (_, owner) = failure.into_parts();
                self.install_frontier(owner)?;
                self.slots.insert(participant_id, slot);
                return Err(StateError::PresentedRefusal(PresentedRefusal::detach(
                    response,
                )));
            }
        };
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
        self.route_fate_occurrence(&make_operation(Vec::new()), self.next_log_sequence)?;
        let (shell, committed) =
            commit_through_barrier(barrier, mode, self.next_log_sequence, &make_operation)?;
        self.shell = Some(shell);
        self.install_frontier(frontier_owner)?;
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
