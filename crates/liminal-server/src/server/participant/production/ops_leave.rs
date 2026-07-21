//! Durable production Leave routing and cold replay through protocol-owned transitions.

use liminal_protocol::lifecycle::{
    AttachSecretProof, BindingState, ConnectionConversationCapacityCommit, DetachCell,
    IdentityState, LeaveFingerprint, LeaveLookupResult, LeaveSecretProof, LiveFrontierOwner,
    LiveLeaveCommit, LiveMember, ObserverProgressProjection, PendingFinalization,
    PendingLeaveCommitParameters, PresentedIdentity, RetainedRecordCharge, RetiredIdentity,
    SemanticConnectionCapacityDecision, VerifiedLeaveRequest, commit_pending_leave_frontier,
    commit_settled_leave_frontier, lookup_leave,
};
use liminal_protocol::wire::{BindingEpoch, LeaveEnvelope, LeaveRequest, LeaveResponse};

use crate::server::participant::dispatch_impact::DispatchImpactAccumulator;

use super::barrier::{ArmOutcome, OperationFacts};
use super::facts::{self, Digest};
use super::fate_occurrence::{FateOccurrenceKey, PendingFinalizerRoute};
use super::frontier::{left_record_charge, terminal_charge as terminal_record_charge};
use super::log::{
    StoredBindingEpoch, StoredFinalizerPresentation, StoredLeaveRequest, StoredLeaveV3,
    StoredOperation, StoredOrdinaryTerminalSource, StoredPendingDiedFinalizer,
};
use super::non_presenting_finalizer::NonPresentingFinalizerCommit;
use super::observer_progress::ObserverProgressSourceMetadata;
use super::outbox_projection::{ReplayedProjectionFacts, project_committed_source};
use super::state::{ConversationAuthority, DurableAppend, StateError};

/// Complete move-only server input to one protocol-owned Leave transition.
struct LeaveTransitionInput {
    owner: LiveFrontierOwner,
    member: LiveMember<Digest>,
    binding: BindingState,
    cell: DetachCell<Digest>,
    verified: VerifiedLeaveRequest<Digest, Digest>,
    next_order: u64,
    next_seq: u64,
}

/// Complete move-only result awaiting one durable Left append.
struct PreparedLeaveCommit {
    owner: LiveFrontierOwner,
    tombstone: RetiredIdentity<Digest, Digest, Digest>,
    observer_projection: Option<ObserverProgressProjection>,
    finalizer: Option<PendingFinalizerRoute>,
    completes_ordinary: bool,
    left_order: u64,
    left_seq: u64,
}

impl ConversationAuthority {
    #[cfg(test)]
    pub(super) fn apply_leave(
        &mut self,
        request: &LeaveRequest,
        operation_facts: &OperationFacts,
        appender: &dyn DurableAppend,
    ) -> Result<ArmOutcome, StateError> {
        let mut impact = DispatchImpactAccumulator::new();
        self.apply_leave_with_impact(request, operation_facts, appender, &mut impact)
    }

    /// Applies one terminal Leave request.
    pub(super) fn apply_leave_with_impact(
        &mut self,
        request: &LeaveRequest,
        operation_facts: &OperationFacts,
        appender: &dyn DurableAppend,
        impact: &mut DispatchImpactAccumulator,
    ) -> Result<ArmOutcome, StateError> {
        let receiving_epoch = BindingEpoch::new(
            operation_facts.receiving_incarnation,
            request.capability_generation,
        );
        let request_verifier = facts::leave_request_verifier(request);
        if let Some(response) = self.classify_leave(request, receiving_epoch, request_verifier)? {
            return Ok(response);
        }
        let capacity = match operation_facts.semantic_connection_capacity() {
            SemanticConnectionCapacityDecision::Respond { limit } => {
                return Ok(ArmOutcome::respond(
                    LeaveResponse::connection_conversation_capacity_exceeded(
                        leave_envelope(request),
                        limit,
                    )
                    .into_server_value(),
                ));
            }
            SemanticConnectionCapacityDecision::Commit(capacity) => capacity,
        };
        let finalizes_pending = self
            .slots
            .get(&request.participant_id)
            .is_some_and(|slot| matches!(slot.binding, BindingState::PendingFinalization(_)));
        let next_immutable = (!finalizes_pending)
            .then_some(self.frontier())
            .flatten()
            .and_then(|owner| {
                owner
                    .frontiers()
                    .sequence()
                    .immutable_candidates()
                    .first()
                    .copied()
            });
        if let Some(candidate) = next_immutable {
            let owner = self.take_frontier()?;
            self.persist_next_marker(candidate, owner, appender, impact)?;
            return self.apply_leave_with_impact(request, operation_facts, appender, impact);
        }
        self.persist_leave(
            request,
            receiving_epoch,
            request_verifier,
            capacity,
            appender,
            impact,
        )
    }

    fn classify_leave(
        &self,
        request: &LeaveRequest,
        receiving_epoch: BindingEpoch,
        request_verifier: Digest,
    ) -> Result<Option<ArmOutcome>, StateError> {
        let envelope = leave_envelope(request);
        let binding_detached = BindingState::Detached;
        let (identity, binding, secret_proof) =
            self.retired.get(&request.participant_id).map_or_else(
                || {
                    self.slots.get(&request.participant_id).map_or(
                        (
                            PresentedIdentity::<Digest, Digest, Digest>::Absent,
                            &binding_detached,
                            LeaveSecretProof::Mismatch,
                        ),
                        |slot| {
                            let proof = if facts::constant_time_eq(
                                &slot.attach_secret.into_bytes(),
                                &request.attach_secret.into_bytes(),
                            ) {
                                LeaveSecretProof::Verified
                            } else {
                                LeaveSecretProof::Mismatch
                            };
                            (
                                PresentedIdentity::<Digest, Digest, Digest>::Live(&slot.member),
                                &slot.binding,
                                proof,
                            )
                        },
                    )
                },
                |tombstone| {
                    let proof = if facts::constant_time_eq(
                        tombstone.leave_request_verifier(),
                        &request_verifier,
                    ) {
                        LeaveSecretProof::Verified
                    } else {
                        LeaveSecretProof::Mismatch
                    };
                    (
                        PresentedIdentity::<Digest, Digest, Digest>::Retired(tombstone),
                        &binding_detached,
                        proof,
                    )
                },
            );
        let response = match lookup_leave(
            identity,
            binding,
            Some(receiving_epoch),
            request,
            secret_proof,
        ) {
            LeaveLookupResult::StaleAuthority(value) => Some(ArmOutcome::respond(
                LeaveResponse::stale_authority(value).into_server_value(),
            )),
            LeaveLookupResult::ParticipantUnknown(_) => Some(ArmOutcome::respond(
                LeaveResponse::participant_unknown(envelope).into_server_value(),
            )),
            LeaveLookupResult::NoBinding(_) => Some(ArmOutcome::respond(
                LeaveResponse::no_binding(envelope).into_server_value(),
            )),
            LeaveLookupResult::LeaveCommitted(value) => Some(ArmOutcome::respond(
                LeaveResponse::leave_committed(value).into_server_value(),
            )),
            LeaveLookupResult::AttemptTokenBodyConflict(_) => Some(ArmOutcome::respond(
                LeaveResponse::attempt_token_body_conflict(
                    request.leave_attempt_token,
                    request.conversation_id,
                    request.participant_id,
                    request.capability_generation,
                )
                .into_server_value(),
            )),
            LeaveLookupResult::Retired(_) => {
                let retired_generation = self
                    .retired
                    .get(&request.participant_id)
                    .ok_or_else(|| StateError::invariant("Leave tombstone disappeared"))?
                    .retired_generation();
                Some(ArmOutcome::respond(
                    LeaveResponse::retired(envelope, retired_generation).into_server_value(),
                ))
            }
            LeaveLookupResult::AuthorizedBound { .. }
            | LeaveLookupResult::AuthorizedDetached { .. } => None,
        };
        Ok(response)
    }

    fn persist_leave(
        &mut self,
        request: &LeaveRequest,
        receiving_epoch: BindingEpoch,
        request_verifier: Digest,
        capacity: ConnectionConversationCapacityCommit,
        appender: &dyn DurableAppend,
        impact: &mut DispatchImpactAccumulator,
    ) -> Result<ArmOutcome, StateError> {
        let source_sequence = self.next_log_sequence;
        let finalizer = self.select_leave_finalizer(request.participant_id)?;
        let prepared = self.prepare_leave_transition(request, request_verifier, finalizer)?;
        let outcome = prepared.tombstone.committed_result().clone();
        let row = StoredLeaveV3 {
            request: StoredLeaveRequest::from(request),
            request_verifier,
            receiving_epoch: StoredBindingEpoch::from(receiving_epoch),
            left_transaction_order: prepared.left_order,
            left_delivery_seq: prepared.left_seq,
            ended_binding_epoch: outcome.ended_binding_epoch().map(StoredBindingEpoch::from),
            prior_terminal_delivery_seq: outcome.prior_terminal_delivery_seq(),
            pending_source_sequence: prepared
                .finalizer
                .map(|route| route.pending_source_sequence),
            finalizer_presentation: prepared
                .finalizer
                .map_or(StoredFinalizerPresentation::PresentEnclosing, |route| {
                    route.presentation
                }),
        };
        let source = StoredOperation::Left { row };
        appender.append(&source, source_sequence)?;
        self.install_frontier(prepared.owner)?;
        self.retired
            .insert(request.participant_id, prepared.tombstone);
        if let Some(projection) = prepared.observer_projection {
            let metadata = ObserverProgressSourceMetadata::leave(
                source_sequence,
                request.conversation_id,
                request.participant_id,
                prepared.left_seq,
            );
            self.record_observer_progress_projection(projection, metadata)?;
        }
        self.observe_replayed_position(prepared.left_order, prepared.left_seq)?;
        self.advance_log_head()?;
        if let Some(projection) = project_committed_source(
            self,
            source_sequence,
            &source,
            ReplayedProjectionFacts {
                superseded_binding_epoch: None,
                marker_delivery: None,
            },
        )? {
            self.record_published_projection(&projection, impact)?;
        }
        self.record_retired(impact);
        self.record_episode_changed(impact);
        if prepared.completes_ordinary {
            self.complete_prepared_ordinary_finalizer(request.participant_id, appender)?;
            self.record_episode_changed(impact);
        }
        Ok(ArmOutcome::committed(
            LeaveResponse::leave_committed(outcome).into_server_value(),
            capacity,
        ))
    }

    fn prepare_leave_transition(
        &mut self,
        request: &LeaveRequest,
        request_verifier: Digest,
        finalizer: Option<PendingFinalizerRoute>,
    ) -> Result<PreparedLeaveCommit, StateError> {
        let slot = self.slots.remove(&request.participant_id).ok_or_else(|| {
            StateError::invariant("authorized Leave slot disappeared before commit")
        })?;
        if !facts::constant_time_eq(
            &slot.attach_secret.into_bytes(),
            &request.attach_secret.into_bytes(),
        ) {
            return Err(StateError::invariant(
                "authorized Leave secret proof changed before commit",
            ));
        }
        let verified = slot
            .member
            .verify_leave_request(
                request,
                AttachSecretProof::Verified,
                request_verifier,
                LeaveFingerprint::new(facts::leave_fingerprint(request)),
            )
            .map_err(|error| {
                StateError::invariant(format!("authorized Leave verification failed: {error:?}"))
            })?;
        let owner = self.take_frontier()?;
        let (owner, completes_ordinary) = match (slot.binding, finalizer) {
            (BindingState::PendingFinalization(PendingFinalization::Died(died)), Some(route)) => {
                self.prepare_pending_died_finalizer(
                    request.participant_id,
                    route.pending_source_sequence,
                    died.commit(self.next_seq),
                    StoredOrdinaryTerminalSource::PendingDiedFinalized {
                        died_source_sequence: route.pending_source_sequence,
                        finalizer: StoredPendingDiedFinalizer::Left {
                            source_sequence: self.next_log_sequence,
                        },
                    },
                    owner,
                )?
            }
            (BindingState::PendingFinalization(_), None) => {
                return Err(StateError::invariant(
                    "pending Leave lost its finalizer route",
                ));
            }
            (BindingState::Bound(_) | BindingState::Detached, Some(_)) => {
                return Err(StateError::invariant(
                    "settled Leave gained a finalizer route",
                ));
            }
            (BindingState::PendingFinalization(PendingFinalization::Detached(_)), Some(_))
            | (BindingState::Bound(_) | BindingState::Detached, None) => (owner, false),
        };
        let mut prepared = transition_leave(
            LeaveTransitionInput {
                owner,
                member: slot.member,
                binding: slot.binding,
                cell: slot.cell,
                verified,
                next_order: self.next_order,
                next_seq: self.next_seq,
            },
            finalizer,
        )?;
        prepared.completes_ordinary = completes_ordinary;
        Ok(prepared)
    }

    /// Replays one v2 Left row through the same protocol-owned Leave
    /// transition and validates every persisted tombstone allocation.
    pub(super) fn replay_leave(&mut self, row: &StoredLeaveV3) -> Result<(), StateError> {
        let source_sequence = self.next_log_sequence;
        let request = row.request.into_request()?;
        let request_verifier = facts::leave_request_verifier(&request);
        if request_verifier != row.request_verifier {
            return Err(StateError::invariant(
                "durable Leave request verifier drifted",
            ));
        }
        let receiving_epoch = row.receiving_epoch.to_epoch()?;
        if receiving_epoch.capability_generation != request.capability_generation {
            return Err(StateError::invariant(
                "durable Leave receiving generation drifted",
            ));
        }
        if row.left_transaction_order < self.next_order {
            return Err(StateError::invariant(
                "durable Leave order is below the next authority allocation",
            ));
        }
        let finalizer = self.select_leave_finalizer(request.participant_id)?;
        let expected_source = finalizer.map(|route| route.pending_source_sequence);
        let expected_presentation = finalizer
            .map_or(StoredFinalizerPresentation::PresentEnclosing, |route| {
                route.presentation
            });
        if row.pending_source_sequence != expected_source
            || row.finalizer_presentation != expected_presentation
        {
            return Err(StateError::invariant(
                "durable Leave finalizer source or presentation drifted",
            ));
        }
        let prepared = self.prepare_leave_transition(&request, request_verifier, finalizer)?;
        let outcome = prepared.tombstone.committed_result();
        let ended_binding_epoch = row
            .ended_binding_epoch
            .map(StoredBindingEpoch::to_epoch)
            .transpose()?;
        if prepared.left_order != row.left_transaction_order
            || prepared.left_seq != row.left_delivery_seq
            || outcome.ended_binding_epoch() != ended_binding_epoch
            || outcome.prior_terminal_delivery_seq() != row.prior_terminal_delivery_seq
        {
            return Err(StateError::invariant(
                "durable Leave tombstone allocation drifted",
            ));
        }
        self.install_frontier(prepared.owner)?;
        self.retired
            .insert(request.participant_id, prepared.tombstone);
        if let Some(projection) = prepared.observer_projection {
            let metadata = ObserverProgressSourceMetadata::leave(
                source_sequence,
                request.conversation_id,
                request.participant_id,
                row.left_delivery_seq,
            );
            self.record_observer_progress_projection(projection, metadata)?;
        }
        self.observe_replayed_position(row.left_transaction_order, row.left_delivery_seq)?;
        self.advance_log_head()
    }

    fn select_leave_finalizer(
        &mut self,
        participant_id: u64,
    ) -> Result<Option<PendingFinalizerRoute>, StateError> {
        let Some(slot) = self.slots.get(&participant_id) else {
            return Err(StateError::invariant(
                "Leave finalizer participant slot is absent",
            ));
        };
        let BindingState::PendingFinalization(pending) = slot.binding else {
            return Ok(None);
        };
        let key = FateOccurrenceKey {
            conversation_id: pending.conversation_id(),
            participant_id: pending.participant_id(),
            binding_epoch: pending.binding_epoch(),
        };
        self.fate_occurrences
            .select_finalizer(key)
            .map(Some)
            .map_err(StateError::from)
    }
}

fn transition_leave(
    input: LeaveTransitionInput,
    finalizer: Option<PendingFinalizerRoute>,
) -> Result<PreparedLeaveCommit, StateError> {
    match input.binding {
        BindingState::PendingFinalization(pending) => {
            let finalizer = finalizer.ok_or_else(|| {
                StateError::invariant("pending Leave omitted its durable finalizer source")
            })?;
            transition_pending_leave(input, pending, finalizer)
        }
        BindingState::Bound(_) | BindingState::Detached => {
            if finalizer.is_some() {
                return Err(StateError::invariant(
                    "settled Leave carried pending-finalizer authority",
                ));
            }
            transition_settled_leave(input)
        }
    }
}

fn transition_settled_leave(
    input: LeaveTransitionInput,
) -> Result<PreparedLeaveCommit, StateError> {
    let left_admission_order = input
        .owner
        .frontiers()
        .planned_settled_leave_admission_order(&input.member, input.binding)
        .map_err(|error| {
            StateError::invariant(format!("settled Leave key planning failed: {error:?}"))
        })?;
    let left_order = left_admission_order.transaction_order();
    if left_order < input.next_order {
        return Err(StateError::invariant(
            "settled Leave planned an already-consumed order",
        ));
    }
    let left_charge =
        RetainedRecordCharge::new(input.next_seq, left_admission_order, left_record_charge());
    let commit = commit_settled_leave_frontier(
        input.owner,
        input.member,
        input.binding,
        input.cell,
        input.verified,
        input.next_seq,
        left_charge,
    )
    .map_err(|error| StateError::invariant(format!("settled Leave failed: {error:?}")))?;
    finish_leave_transition(commit, None, left_order, input.next_seq)
}

fn transition_pending_leave(
    input: LeaveTransitionInput,
    pending: PendingFinalization,
    finalizer: PendingFinalizerRoute,
) -> Result<PreparedLeaveCommit, StateError> {
    let left_admission_order = input
        .owner
        .frontiers()
        .planned_pending_leave_admission_order(&input.member, pending)
        .map_err(|error| {
            StateError::invariant(format!("pending Leave key planning failed: {error:?}"))
        })?;
    let left_order = left_admission_order.transaction_order();
    if left_order < input.next_order {
        return Err(StateError::invariant(
            "pending Leave planned an already-consumed order",
        ));
    }
    let candidates = input.owner.frontiers().sequence().immutable_candidates();
    let [terminal_candidate] = candidates else {
        return Err(StateError::invariant(
            "pending Leave does not own one exact terminal candidate",
        ));
    };
    let terminal_seq = terminal_candidate.delivery_seq();
    if terminal_candidate.admission_order() != pending.admission_order()
        || terminal_seq != input.next_seq
    {
        return Err(StateError::invariant(
            "pending Leave terminal allocation drifted",
        ));
    }
    let left_seq = terminal_seq
        .checked_add(1)
        .ok_or_else(|| StateError::invariant("pending Leave sequence exhausted before Left"))?;
    let terminal_charge = RetainedRecordCharge::new(
        terminal_seq,
        pending.admission_order(),
        terminal_record_charge(
            pending.conversation_id(),
            pending.participant_id(),
            pending.binding_epoch(),
            pending.admission_order().transaction_order(),
            terminal_seq,
        )?,
    );
    let left_charge =
        RetainedRecordCharge::new(left_seq, left_admission_order, left_record_charge());
    let commit = commit_pending_leave_frontier(
        input.owner,
        input.member,
        pending,
        input.cell,
        input.verified,
        PendingLeaveCommitParameters {
            terminal_delivery_seq: terminal_seq,
            left_delivery_seq: left_seq,
        },
        [terminal_charge, left_charge],
    )
    .map_err(|error| StateError::invariant(format!("pending Leave failed: {error:?}")))?;
    finish_leave_transition(commit, Some(finalizer), left_order, left_seq)
}

fn finish_leave_transition(
    commit: LiveLeaveCommit<Digest, Digest, Digest>,
    finalizer: Option<PendingFinalizerRoute>,
    left_order: u64,
    left_seq: u64,
) -> Result<PreparedLeaveCommit, StateError> {
    let non_presenting = matches!(
        finalizer.map(|route| route.presentation),
        Some(StoredFinalizerPresentation::ConsumeRecoveredReservation { .. })
    );
    let (identity, owner, observer_projection) = if non_presenting {
        let (identity, owner) = commit.into_parts();
        let commit = NonPresentingFinalizerCommit::new(identity, owner);
        let (identity, owner) = commit.into_parts();
        (identity, owner, None)
    } else {
        let projection = commit
            .observer_progress_projection()
            .ok_or_else(|| StateError::invariant("Leave commit did not retire its identity"))?;
        let (identity, owner) = commit.into_parts();
        (identity, owner, Some(projection))
    };
    let IdentityState::Retired(tombstone) = identity else {
        return Err(StateError::invariant("Leave retained a live identity"));
    };
    Ok(PreparedLeaveCommit {
        owner,
        tombstone,
        observer_projection,
        finalizer,
        completes_ordinary: false,
        left_order,
        left_seq,
    })
}

/// Builds the echo envelope of one Leave request.
const fn leave_envelope(request: &LeaveRequest) -> LeaveEnvelope {
    LeaveEnvelope {
        conversation_id: request.conversation_id,
        participant_id: request.participant_id,
        capability_generation: request.capability_generation,
        leave_attempt_token: request.leave_attempt_token,
    }
}
