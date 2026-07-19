//! Ordinary record admission, marker drain, and their exact cold replay.
//!
//! Every authorized transition temporarily consumes the conversation's
//! validated live frontier owner. Marker-drain and record commits cross one
//! append/flush boundary before the replacement owner, causal counters, or
//! response become observable. Refusals remain protocol-selected and return
//! the complete unchanged owner.

use liminal_protocol::algebra::ResourceVector;
use liminal_protocol::lifecycle::{
    BindingState, CapacityCounter, ConnectionConversationTracking, ImmutableSequenceCandidate,
    LiveFrontierOwner, MarkerDeliveryProjection, PresentedIdentity, RecordAdmissionDecision,
    RecordAdmissionPrestate, RetainedRecordCharge, SemanticConnectionCapacityDecision,
    apply_record_admission as select_record_admission, classify_record_admission_binding,
    drain_next_marker,
};
use liminal_protocol::wire::{
    BindingEpoch, ParticipantDelivery, RecordAdmission, RecordAdmissionResponse,
};

use crate::config::types::ParticipantConfig;

use super::barrier::{ArmOutcome, OperationFacts};
use super::facts::Digest;
use super::frontier::{ordinary_projection_limits, ordinary_record_charge};
use super::log::{
    StoredBindingEpoch, StoredMarkerDrain, StoredOperation, StoredRecordAdmission,
    StoredRecordAdmissionRequest, StoredResourceVector, StoredRetainedCharge,
};
use super::state::{ConversationAuthority, DurableAppend, StateError};

impl ConversationAuthority {
    /// Applies one ordinary record admission.
    ///
    /// Binding lookup (stages 2-5), stage-6 connection capacity, and all
    /// frontier-dependent admission outcomes run through protocol selectors.
    /// Commit and mandatory marker-drain arms publish state only after their
    /// complete durable rows have appended and flushed.
    pub(super) fn apply_record_admission(
        &mut self,
        request: &RecordAdmission,
        operation_facts: &OperationFacts,
        config: &ParticipantConfig,
        appender: &dyn DurableAppend,
    ) -> Result<ArmOutcome, StateError> {
        let receiving_epoch = BindingEpoch::new(
            operation_facts.receiving_incarnation,
            request.capability_generation,
        );
        let binding_detached = BindingState::Detached;
        let (identity, binding) = self.slots.get(&request.participant_id).map_or(
            (
                PresentedIdentity::<Digest, Digest, Digest>::Absent,
                &binding_detached,
            ),
            |slot| {
                (
                    PresentedIdentity::<Digest, Digest, Digest>::Live(&slot.member),
                    &slot.binding,
                )
            },
        );
        if let Some(response) =
            classify_record_admission_binding(identity, binding, receiving_epoch, request)
        {
            return Ok(ArmOutcome::respond(response.into_server_value()));
        }
        if let SemanticConnectionCapacityDecision::Respond { limit } =
            operation_facts.semantic_connection_capacity()
        {
            return Ok(ArmOutcome::respond(
                RecordAdmissionResponse::connection_conversation_capacity_exceeded(
                    record_envelope(request),
                    limit,
                )
                .into_server_value(),
            ));
        }

        let owner = self.take_frontier()?;
        let retained_record_limit = owner.retained_record_limit();
        let (frontiers, closure_accounting, retained_charges, _) = owner.into_parts();
        let slot = self
            .slots
            .get(&request.participant_id)
            .ok_or_else(|| StateError::invariant("authorized record slot disappeared"))?;
        let encoded_record_charge = ordinary_record_charge(request)?;
        let prestate = RecordAdmissionPrestate::new(
            request.clone(),
            PresentedIdentity::<Digest, Digest, Digest>::Live(&slot.member),
            &slot.binding,
            receiving_epoch,
            operation_facts.connection_tracking,
            operation_facts.connection_capacity,
            closure_accounting,
            ResourceVector::new(
                config.max_ordinary_record_entries,
                config.max_ordinary_record_bytes,
            ),
            frontiers,
            retained_charges,
            self.observer_progress,
            ordinary_projection_limits(config),
        );
        match select_record_admission(prestate, encoded_record_charge) {
            RecordAdmissionDecision::Respond(refusal) => {
                let (response, unchanged) = refusal.into_parts();
                let (owner, _, _) = LiveFrontierOwner::from_unchanged_record_admission(
                    unchanged,
                    retained_record_limit,
                );
                self.install_frontier(owner);
                Ok(ArmOutcome::respond(response.into_server_value()))
            }
            RecordAdmissionDecision::DrainFirst(drain) => {
                let (candidate, unchanged) = drain.into_parts();
                let (owner, request, _) = LiveFrontierOwner::from_unchanged_record_admission(
                    unchanged,
                    retained_record_limit,
                );
                self.persist_marker_and_retry(
                    candidate,
                    owner,
                    &request,
                    operation_facts,
                    config,
                    appender,
                )
            }
            RecordAdmissionDecision::Fault(failure) => {
                let (fault, _) = failure.into_parts();
                Err(StateError::invariant(format!(
                    "record admission protocol fault: {fault:?}"
                )))
            }
            RecordAdmissionDecision::Commit(commit) => self.persist_record_commit(
                *commit,
                receiving_epoch,
                retained_record_limit,
                appender,
            ),
        }
    }

    fn persist_marker_and_retry(
        &mut self,
        candidate: ImmutableSequenceCandidate,
        owner: LiveFrontierOwner,
        request: &RecordAdmission,
        operation_facts: &OperationFacts,
        config: &ParticipantConfig,
        appender: &dyn DurableAppend,
    ) -> Result<ArmOutcome, StateError> {
        self.persist_next_marker(candidate, owner, appender)?;
        self.apply_record_admission(request, operation_facts, config, appender)
    }

    pub(super) fn persist_next_marker(
        &mut self,
        candidate: ImmutableSequenceCandidate,
        owner: LiveFrontierOwner,
        appender: &dyn DurableAppend,
    ) -> Result<(), StateError> {
        let next_seq =
            candidate
                .delivery_seq()
                .checked_add(1)
                .ok_or(StateError::AllocationExhausted {
                    domain: "delivery sequence after marker drain",
                })?;
        let retained_record_limit = owner.retained_record_limit();
        let marker = canonical_marker_bytes(candidate)?;
        let marker_bytes = u64::try_from(marker.len())
            .map_err(|_| StateError::invariant("canonical marker row length exceeds u64"))?;
        let marker_charge = RetainedRecordCharge::new(
            candidate.delivery_seq(),
            candidate.admission_order(),
            ResourceVector::new(1, marker_bytes),
        );
        let (frontiers, accounting, retained_charges, _) = owner.into_parts();
        let commit = drain_next_marker(frontiers, accounting, retained_charges, marker_charge)
            .map_err(|error| {
                StateError::invariant(format!("mandatory marker drain failed: {error:?}"))
            })?;
        let row = StoredMarkerDrain {
            marker,
            retained_charge: stored_retained_charge(&marker_charge),
            resulting_retained_charges: commit
                .retained_charges()
                .iter()
                .map(stored_retained_charge)
                .collect(),
            successor: format!("{:?}", commit.marker_successor()).into_bytes(),
        };
        let (owner, _, projection) =
            LiveFrontierOwner::from_marker_drain(commit, retained_record_limit);
        validate_marker_projection(self.conversation_id, &projection)?;
        #[cfg(test)]
        {
            self.last_marker_projection = Some(projection.delivery().clone());
        }
        appender.append(
            &StoredOperation::MarkerDrained { row },
            self.next_log_sequence,
        )?;
        self.install_frontier(owner);
        self.next_seq = self.next_seq.max(next_seq);
        self.advance_log_head()?;
        Ok(())
    }

    fn persist_record_commit(
        &mut self,
        commit: liminal_protocol::lifecycle::RecordAdmissionCommit,
        receiving_epoch: BindingEpoch,
        retained_record_limit: u64,
        appender: &dyn DurableAppend,
    ) -> Result<ArmOutcome, StateError> {
        let persistence = commit.into_persistence_parts();
        let admission_order = persistence.record.admission_order();
        let connection_capacity = persistence.connection_capacity;
        let row = StoredRecordAdmission {
            request: StoredRecordAdmissionRequest::from(persistence.record.request()),
            receiving_epoch: StoredBindingEpoch::from(receiving_epoch),
            transaction_order: admission_order.transaction_order(),
            delivery_seq: persistence.record.delivery_seq(),
            encoded_record_charge: StoredResourceVector {
                entries: persistence.record.encoded_record_charge().entries,
                bytes: persistence.record.encoded_record_charge().bytes,
            },
            resulting_connection_count: connection_capacity.resulting().occupied(),
            newly_tracked: connection_capacity.newly_tracked(),
            resulting_retained_charges: persistence
                .retained_charges
                .iter()
                .map(stored_retained_charge)
                .collect(),
            resulting_closure_accounting: format!("{:?}", persistence.accounting).into_bytes(),
        };
        appender.append(
            &StoredOperation::RecordAdmission { row },
            self.next_log_sequence,
        )?;
        let response = persistence.outcome.clone();
        let order = persistence.order.major();
        let sequence = persistence.record.delivery_seq();
        let owner = LiveFrontierOwner::from_record_admission_persistence(
            persistence,
            retained_record_limit,
        );
        self.install_frontier(owner);
        self.observe_replayed_position(order, sequence)?;
        self.advance_log_head()?;
        Ok(ArmOutcome::committed(
            RecordAdmissionResponse::record_committed(response).into_server_value(),
            connection_capacity,
        ))
    }

    /// Replays one mandatory v2 marker drain through the protocol-owned drain
    /// and verifies its canonical row, successor, and complete retained charges.
    pub(super) fn replay_marker_drain(
        &mut self,
        row: &StoredMarkerDrain,
    ) -> Result<ParticipantDelivery, StateError> {
        let owner = self.take_frontier()?;
        let retained_record_limit = owner.retained_record_limit();
        let candidate = owner
            .frontiers()
            .sequence()
            .immutable_candidates()
            .first()
            .copied()
            .ok_or_else(|| StateError::invariant("durable marker drain has no candidate"))?;
        let next_seq =
            candidate
                .delivery_seq()
                .checked_add(1)
                .ok_or(StateError::AllocationExhausted {
                    domain: "delivery sequence after durable marker drain",
                })?;
        let marker = canonical_marker_bytes(candidate)?;
        if marker != row.marker {
            return Err(StateError::invariant("durable marker row drifted"));
        }
        let marker_bytes = u64::try_from(marker.len())
            .map_err(|_| StateError::invariant("canonical marker row length exceeds u64"))?;
        let marker_charge = RetainedRecordCharge::new(
            candidate.delivery_seq(),
            candidate.admission_order(),
            ResourceVector::new(1, marker_bytes),
        );
        if stored_retained_charge(&marker_charge) != row.retained_charge {
            return Err(StateError::invariant("durable marker charge drifted"));
        }
        let (frontiers, accounting, retained_charges, _) = owner.into_parts();
        let commit = drain_next_marker(frontiers, accounting, retained_charges, marker_charge)
            .map_err(|error| {
                StateError::invariant(format!("durable marker drain failed: {error:?}"))
            })?;
        let resulting: Vec<_> = commit
            .retained_charges()
            .iter()
            .map(stored_retained_charge)
            .collect();
        if resulting != row.resulting_retained_charges
            || format!("{:?}", commit.marker_successor()).into_bytes() != row.successor
        {
            return Err(StateError::invariant(
                "durable marker drain poststate audit drifted",
            ));
        }
        let (owner, _, projection) =
            LiveFrontierOwner::from_marker_drain(commit, retained_record_limit);
        validate_marker_projection(self.conversation_id, &projection)?;
        self.install_frontier(owner);
        self.next_seq = self.next_seq.max(next_seq);
        self.advance_log_head()?;
        Ok(projection.into_delivery())
    }

    /// Replays one committed v2 `RecordAdmission` through the same total selector
    /// and verifies every persisted allocation/charge audit before publication.
    pub(super) fn replay_record_admission(
        &mut self,
        row: &StoredRecordAdmission,
        config: &ParticipantConfig,
    ) -> Result<(), StateError> {
        let request = row.request.clone().into_request()?;
        let receiving_epoch = row.receiving_epoch.to_epoch()?;
        let tracking = if row.newly_tracked {
            ConnectionConversationTracking::Untracked
        } else {
            ConnectionConversationTracking::AlreadyTracked
        };
        let occupied = if row.newly_tracked {
            row.resulting_connection_count
                .checked_sub(1)
                .ok_or_else(|| {
                    StateError::invariant(
                        "newly tracked durable record has zero resulting occupancy",
                    )
                })?
        } else {
            row.resulting_connection_count
        };
        let capacity =
            CapacityCounter::try_new(config.max_semantic_conversations_per_connection, occupied)
                .map_err(|error| {
                    StateError::invariant(format!("durable record capacity is invalid: {error:?}"))
                })?;
        let owner = self.take_frontier()?;
        let retained_record_limit = owner.retained_record_limit();
        let (frontiers, closure_accounting, retained_charges, _) = owner.into_parts();
        let slot = self
            .slots
            .get(&request.participant_id)
            .ok_or_else(|| StateError::invariant("durable record participant is absent"))?;
        let encoded_record_charge = ordinary_record_charge(&request)?;
        if encoded_record_charge.entries != row.encoded_record_charge.entries
            || encoded_record_charge.bytes != row.encoded_record_charge.bytes
        {
            return Err(StateError::invariant(
                "durable record canonical charge drifted",
            ));
        }
        let prestate = RecordAdmissionPrestate::new(
            request,
            PresentedIdentity::<Digest, Digest, Digest>::Live(&slot.member),
            &slot.binding,
            receiving_epoch,
            tracking,
            capacity,
            closure_accounting,
            ResourceVector::new(
                config.max_ordinary_record_entries,
                config.max_ordinary_record_bytes,
            ),
            frontiers,
            retained_charges,
            self.observer_progress,
            ordinary_projection_limits(config),
        );
        let RecordAdmissionDecision::Commit(commit) =
            select_record_admission(prestate, encoded_record_charge)
        else {
            return Err(StateError::invariant(
                "durable committed record did not replay as Commit",
            ));
        };
        let persistence = commit.into_persistence_parts();
        let order = persistence.record.admission_order().transaction_order();
        let sequence = persistence.record.delivery_seq();
        let retained: Vec<_> = persistence
            .retained_charges
            .iter()
            .map(stored_retained_charge)
            .collect();
        if order != row.transaction_order
            || sequence != row.delivery_seq
            || retained != row.resulting_retained_charges
            || persistence.connection_capacity.resulting().occupied()
                != row.resulting_connection_count
            || persistence.connection_capacity.newly_tracked() != row.newly_tracked
            || format!("{:?}", persistence.accounting).into_bytes()
                != row.resulting_closure_accounting
        {
            return Err(StateError::invariant(
                "durable RecordAdmission poststate audit drifted",
            ));
        }
        let owner = LiveFrontierOwner::from_record_admission_persistence(
            persistence,
            retained_record_limit,
        );
        self.install_frontier(owner);
        self.observe_replayed_position(order, sequence)?;
        self.advance_log_head()
    }
}

fn validate_marker_projection(
    conversation_id: u64,
    projection: &MarkerDeliveryProjection,
) -> Result<(), StateError> {
    if projection.delivery().conversation_id != conversation_id {
        return Err(StateError::invariant(
            "protocol marker projection belongs to another conversation",
        ));
    }
    Ok(())
}

pub(super) fn canonical_marker_bytes(
    candidate: ImmutableSequenceCandidate,
) -> Result<Vec<u8>, StateError> {
    match candidate {
        ImmutableSequenceCandidate::Marker(marker) => Ok(format!(
            "MarkerCandidateAuthority {{ delivery_seq: {:?}, admission_order: {:?}, target_binding: {:?}, provenance: {:?}, current_owner: {:?} }}",
            marker.delivery_seq,
            marker.admission_order,
            marker.target_binding,
            marker.provenance,
            marker.current_owner,
        )
        .into_bytes()),
        ImmutableSequenceCandidate::BindingTerminal { .. } => Err(StateError::invariant(
            "DrainFirst selected a binding terminal instead of marker work",
        )),
    }
}

const fn stored_retained_charge(
    charge: &liminal_protocol::lifecycle::RetainedRecordCharge,
) -> StoredRetainedCharge {
    let order = charge.admission_order();
    StoredRetainedCharge {
        delivery_seq: charge.delivery_seq(),
        transaction_order: order.transaction_order(),
        candidate_phase: order.candidate_phase() as u8,
        participant_id: order.participant_index(),
        charge: StoredResourceVector {
            entries: charge.encoded_charge().entries,
            bytes: charge.encoded_charge().bytes,
        },
    }
}

/// Builds the echo envelope of one ordinary record admission.
const fn record_envelope(
    request: &RecordAdmission,
) -> liminal_protocol::wire::RecordAdmissionEnvelope {
    liminal_protocol::wire::RecordAdmissionEnvelope {
        conversation_id: request.conversation_id,
        participant_id: request.participant_id,
        capability_generation: request.capability_generation,
        record_admission_attempt_token: request.record_admission_attempt_token,
    }
}
