//! Detach, cumulative-ack, and marker-ack arms plus the cold replay driver.
//!
//! Same discipline and error contract as [`super::ops_enroll`] and
//! [`super::ops_attach`]: shared lookups
//! classify, crate transitions commit, the A3 aggregate barrier orders every
//! shell event behind its durable append, and request-bound response
//! authorities carry every reply.

use liminal_protocol::lifecycle::{
    AggregateOperationDecision, BindingState, CommittedBindingTerminalPosition, DetachCell,
    DetachLookupContext, DetachLookupResult, DetachTokenResolution, MarkerAckDecision,
    MarkerProofState, ParticipantAckDecision, PresentedIdentity, ResolvedIdentity,
    SemanticConnectionCapacityDecision, commit_detach, decide_detached_operation, lookup_detach,
};
use liminal_protocol::lifecycle::{apply_marker_ack, apply_participant_ack};
use liminal_protocol::wire::{
    BindingEpoch, DetachEnvelope, DetachRequest, DetachResponse, MarkerAck, MarkerAckEnvelope,
    MarkerAckResponse, ParticipantAck, ParticipantAckEnvelope, ParticipantAckResponse,
    ServerDiscriminant, ServerValue,
};

use super::barrier::{ArmOutcome, CommitMode, OperationFacts, commit_through_barrier};
use super::facts::{self, Digest};
use super::log::{
    OperationLog, StoredAck, StoredBindingEpoch, StoredDetachRequest, StoredOperation,
};
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

/// Builds the echo envelope of one detach request.
const fn detach_envelope(request: &DetachRequest) -> DetachEnvelope {
    DetachEnvelope {
        conversation_id: request.conversation_id,
        participant_id: request.participant_id,
        capability_generation: request.capability_generation,
        detach_attempt_token: request.detach_attempt_token,
    }
}
