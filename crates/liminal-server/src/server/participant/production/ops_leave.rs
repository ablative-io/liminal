//! Durable production Leave routing and cold replay.
//!
//! Lookup remains protocol-owned; an authorized transition consumes the live
//! frontier owner, appends one complete v2 `Left` row, and only then publishes
//! the permanent tombstone and replacement frontier.

use liminal_protocol::lifecycle::{
    AttachSecretProof, BindingState, ConnectionConversationCapacityCommit, DetachCell,
    IdentityState, LeaveFingerprint, LeaveLookupResult, LeaveSecretProof, LiveFrontierOwner,
    LiveLeaveCommit, LiveMember, ObserverProgressProjection, PendingFinalization,
    PendingLeaveCommitParameters, PresentedIdentity, RetainedRecordCharge, RetiredIdentity,
    SemanticConnectionCapacityDecision, VerifiedLeaveRequest, commit_pending_leave_frontier,
    commit_settled_leave_frontier, lookup_leave,
};
use liminal_protocol::wire::{BindingEpoch, LeaveEnvelope, LeaveRequest, LeaveResponse};

use super::barrier::{ArmOutcome, OperationFacts};
use super::facts::{self, Digest};
use super::frontier::{left_record_charge, terminal_charge as terminal_record_charge};
use super::log::{StoredBindingEpoch, StoredLeave, StoredLeaveRequest, StoredOperation};
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
    observer_projection: ObserverProgressProjection,
    left_order: u64,
    left_seq: u64,
}

impl ConversationAuthority {
    /// Applies one terminal Leave request.
    pub(super) fn apply_leave(
        &mut self,
        request: &LeaveRequest,
        operation_facts: &OperationFacts,
        appender: &dyn DurableAppend,
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
        self.persist_leave(
            request,
            receiving_epoch,
            request_verifier,
            capacity,
            appender,
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
    ) -> Result<ArmOutcome, StateError> {
        let prepared = self.prepare_leave_transition(request, request_verifier)?;
        let outcome = prepared.tombstone.committed_result().clone();
        let row = StoredLeave {
            request: StoredLeaveRequest::from(request),
            request_verifier,
            receiving_epoch: StoredBindingEpoch::from(receiving_epoch),
            left_transaction_order: prepared.left_order,
            left_delivery_seq: prepared.left_seq,
            ended_binding_epoch: outcome.ended_binding_epoch().map(StoredBindingEpoch::from),
            prior_terminal_delivery_seq: outcome.prior_terminal_delivery_seq(),
        };
        appender.append(&StoredOperation::Left { row }, self.next_log_sequence)?;
        self.install_frontier(prepared.owner);
        self.retired
            .insert(request.participant_id, prepared.tombstone);
        self.record_observer_progress_projection(prepared.observer_projection);
        self.next_order = self.next_order.max(prepared.left_order.saturating_add(1));
        self.next_seq = prepared.left_seq.saturating_add(1);
        self.advance_log_head()?;
        Ok(ArmOutcome::committed(
            LeaveResponse::leave_committed(outcome).into_server_value(),
            capacity,
        ))
    }

    fn prepare_leave_transition(
        &mut self,
        request: &LeaveRequest,
        request_verifier: Digest,
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
        transition_leave(LeaveTransitionInput {
            owner: self.take_frontier()?,
            member: slot.member,
            binding: slot.binding,
            cell: slot.cell,
            verified,
            next_order: self.next_order,
            next_seq: self.next_seq,
        })
    }

    /// Replays one v2 Left row through the same protocol-owned Leave
    /// transition and validates every persisted tombstone allocation.
    pub(super) fn replay_leave(&mut self, row: &StoredLeave) -> Result<(), StateError> {
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
        let prepared = self.prepare_leave_transition(&request, request_verifier)?;
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
        self.install_frontier(prepared.owner);
        self.retired
            .insert(request.participant_id, prepared.tombstone);
        self.record_observer_progress_projection(prepared.observer_projection);
        self.next_order = self
            .next_order
            .max(row.left_transaction_order.saturating_add(1));
        self.next_seq = row.left_delivery_seq.saturating_add(1);
        self.advance_log_head()
    }
}

fn transition_leave(input: LeaveTransitionInput) -> Result<PreparedLeaveCommit, StateError> {
    match input.binding {
        BindingState::PendingFinalization(pending) => transition_pending_leave(input, pending),
        BindingState::Bound(_) | BindingState::Detached => transition_settled_leave(input),
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
    finish_leave_transition(commit, left_order, input.next_seq)
}

fn transition_pending_leave(
    input: LeaveTransitionInput,
    pending: PendingFinalization,
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
    finish_leave_transition(commit, left_order, left_seq)
}

fn finish_leave_transition(
    commit: LiveLeaveCommit<Digest, Digest, Digest>,
    left_order: u64,
    left_seq: u64,
) -> Result<PreparedLeaveCommit, StateError> {
    let observer_projection = commit
        .observer_progress_projection()
        .ok_or_else(|| StateError::invariant("Leave commit did not retire its identity"))?;
    let (identity, owner) = commit.into_parts();
    let IdentityState::Retired(tombstone) = identity else {
        return Err(StateError::invariant("Leave retained a live identity"));
    };
    Ok(PreparedLeaveCommit {
        owner,
        tombstone,
        observer_projection,
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
