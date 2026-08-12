//! Cumulative-ack and marker-ack arms.
//!
//! Same discipline and error contract as the sibling operation arms: the
//! crate's total ack selectors classify and commit, the frozen stage-6
//! connection-conversation capacity gate runs at its register position, and
//! request-bound response authorities carry every reply.

use liminal::durability::bridge::block_on;

use liminal_protocol::lifecycle::{
    BindingState, MarkerAckCommit, MarkerAckDecision, MarkerProofState, ParticipantAckCommit,
    ParticipantAckDecision, PresentedIdentity, RecipientAckObligations, RetainedCausalRecordKind,
    SemanticConnectionCapacityDecision, apply_marker_ack, apply_marker_ack_frontier,
    apply_participant_ack_frontier, apply_participant_ack_with_obligations,
};
use liminal_protocol::wire::{
    BindingEpoch, DeliverySeq, MarkerAck, MarkerAckEnvelope, MarkerAckResponse, ParticipantAck,
    ParticipantAckEnvelope, ParticipantAckResponse, ParticipantId, ServerDiscriminant, ServerValue,
};

use crate::server::participant::dispatch_impact::DispatchImpactAccumulator;

use super::barrier::{ArmOutcome, OperationFacts};
use super::facts::Digest;
use super::log::{StoredAck, StoredBindingEpoch, StoredOperation};
use super::marker_progress::{marker_delivery_progress, marker_replay_progress};
use super::observer_progress::ObserverProgressSourceMetadata;
use super::outbox_log::{OutboxLog, OutboxRow, StoredMarkerAckCommitted};
use super::state::{ConversationAuthority, DurableAppend, PendingBindingFate, Slot, StateError};

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

    /// Replays one committed zero-debt row with testimony sealed at its exact
    /// outbox merge boundary.
    pub(super) fn replay_zero_debt_ack_row(
        &mut self,
        request: StoredAck,
        receiving_epoch: StoredBindingEpoch,
        contiguously_available_through: u64,
        ack_obligations: Option<(RecipientAckObligations, u64)>,
    ) -> Result<(), StateError> {
        let (obligations, reconciled_available_through) = ack_obligations.ok_or_else(|| {
            StateError::invariant("zero-debt ack replay is missing recipient obligations")
        })?;
        self.replay_zero_debt_ack(
            request,
            receiving_epoch,
            contiguously_available_through,
            reconciled_available_through,
            &obligations,
        )
    }

    /// Replays one committed zero-debt ack entry from its stored inputs.
    fn replay_zero_debt_ack(
        &mut self,
        request: StoredAck,
        receiving_epoch: StoredBindingEpoch,
        contiguously_available_through: u64,
        reconciled_available_through: u64,
        obligations: &RecipientAckObligations,
    ) -> Result<(), StateError> {
        if contiguously_available_through != reconciled_available_through {
            return Err(StateError::invariant(format!(
                "durable zero-debt ack availability {contiguously_available_through} differs from reconciled recipient availability {reconciled_available_through}"
            )));
        }
        let request = request.to_request()?;
        let outcome = self.ack_commit(
            &request,
            receiving_epoch,
            obligations,
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

    fn commit_marker_ack(
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

    /// Replays one extension `MarkerAck` through the authoritative selector and
    /// checks the complete stored commit census before installing any state.
    pub(super) fn replay_marker_ack_extension(
        &mut self,
        row: &StoredMarkerAckCommitted,
    ) -> Result<(), StateError> {
        if row.request.conversation_id != self.conversation_id
            || row.offered_marker_delivery_seq != row.request.marker_delivery_seq
            || row.receiving_binding_epoch != row.delivered_binding_epoch
        {
            return Err(StateError::invariant(
                "stored MarkerAck request and delivery witness disagree",
            ));
        }
        let progress = marker_replay_progress(self, row)?;
        let identity = self
            .slots
            .get(&row.request.participant_id)
            .map_or(PresentedIdentity::Absent, |slot| {
                PresentedIdentity::<Digest, Digest, Digest>::Live(&slot.member)
            });
        let detached = BindingState::Detached;
        let binding = self
            .slots
            .get(&row.request.participant_id)
            .map_or(&detached, |slot| &slot.binding);
        let cursor = self
            .slots
            .get(&row.request.participant_id)
            .map_or(0, |slot| slot.member.cursor());
        // A log written before ordinary acks retired crossed marker anchors
        // can hold an ordinary-ack row that crosses this marker FOLLOWED by
        // this marker-ack row — both committed under the old accounting.
        // Replaying the ordinary row through the fixed applies already
        // advanced the cursor to (or past) the marker and retired its anchor;
        // re-committing this row would retire the anchor a second time and
        // kill the load. The acceptance this row witnesses is already durably
        // reflected in the replayed state, so it is a no-op here. The sealed
        // binding-fate token is safe to skip with it: the crossing row
        // progressed it, and token replay is idempotent.
        if cursor >= row.offered_marker_delivery_seq {
            return Ok(());
        }
        let marker_state = MarkerProofState::new(
            cursor,
            false,
            Some(row.offered_marker_delivery_seq),
            row.delivered_binding_epoch,
            Some(progress),
        );
        let MarkerAckDecision::Commit(commit) = apply_marker_ack(
            identity,
            binding,
            row.receiving_binding_epoch,
            &row.request,
            &marker_state,
        ) else {
            return Err(StateError::invariant(
                "stored MarkerAck replayed to a non-commit decision",
            ));
        };
        if commit.canonical_request() != row.request
            || commit.receiving_binding_epoch() != row.receiving_binding_epoch
            || commit.offered_marker_delivery_seq() != row.offered_marker_delivery_seq
            || commit.delivered_binding_epoch() != row.delivered_binding_epoch
            || commit.from_cursor() != row.from_cursor
            || commit.resulting_cursor() != row.resulting_cursor
        {
            return Err(StateError::invariant(
                "stored MarkerAck post-transition audit drifted",
            ));
        }
        let observer_projection = commit.observer_progress_projection();
        let metadata = ObserverProgressSourceMetadata::marker_ack(
            row.base_log_head,
            row.extension_sequence,
            row.request.conversation_id,
            row.request.participant_id,
            row.request.marker_delivery_seq,
            row.resulting_cursor,
        );
        let transitioned =
            apply_marker_ack_frontier(self.take_frontier()?, commit).map_err(|failure| {
                StateError::invariant(format!(
                    "stored MarkerAck frontier transition failed: {:?}",
                    failure.error()
                ))
            })?;
        let (commit, frontier) = transitioned.into_parts();
        let slot = self
            .slots
            .get_mut(&row.request.participant_id)
            .ok_or_else(|| StateError::invariant("stored MarkerAck participant is absent"))?;
        // SITE TWO of `#26`, and the one that made the fault survive restarts.
        // This is the load path — `ops_session_replay` -> `recipient_ack_obligations`
        // -> `outbox_replay` -> here — so without this call a boot did not clear
        // the frozen token, it REBUILT it from durable rows every time.
        progress_pending_marker_binding_fate(slot, &commit)?;
        let outcome = commit.apply_to(&mut slot.member).map_err(|error| {
            StateError::invariant(format!("stored MarkerAck cursor commit failed: {error:?}"))
        })?;
        let request = outcome.request();
        if request.conversation_id != row.request.conversation_id
            || request.participant_id != row.request.participant_id
            || request.capability_generation != row.request.capability_generation
            || request.marker_delivery_seq != row.request.marker_delivery_seq
        {
            return Err(StateError::invariant(
                "stored MarkerAck outcome request drifted",
            ));
        }
        self.install_frontier(frontier)?;
        self.record_observer_progress_projection(observer_projection, metadata)?;
        Ok(())
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
const fn marker_ack_envelope(request: &MarkerAck) -> MarkerAckEnvelope {
    MarkerAckEnvelope {
        conversation_id: request.conversation_id,
        participant_id: request.participant_id,
        capability_generation: request.capability_generation,
        marker_delivery_seq: request.marker_delivery_seq,
    }
}

/// Moves the sealed binding-fate token in step with a MARKER acknowledgement.
///
/// The sibling of `progress_pending_binding_fate`, for the path that never had
/// one. A member with no pending fate token is the ordinary case and returns
/// `Ok` untouched — the token only exists after a `CredentialAttach` mints it.
///
/// # ⚠ WHY THIS REFUSAL IS WORDED DIFFERENTLY FROM ITS SIBLING
///
/// The sibling raises `ack cursor commit disagrees with sealed binding-fate
/// authority` — the string the `#26` defect surfaces, because under the defect
/// it is the ORDINARY ack that gets refused against a token this path failed to
/// move. If this function reused that wording, a future refusal from the marker
/// path would be indistinguishable from the original bug in every log and every
/// assertion, and "the fix regressed" would read exactly like "the fix was never
/// applied". The distinct prefix keeps the two separable at a glance.
///
/// ⛔ It does NOT make the string safe as a gate signal. A refusal is a refusal
/// in both a healthy and a broken tree; only STATE — whether the next ordinary
/// ack COMMITS — discriminates, and the guarding units are built on that.
fn progress_pending_marker_binding_fate(
    slot: &mut Slot,
    commit: &MarkerAckCommit,
) -> Result<(), StateError> {
    let Some(pending) = slot.binding_fate.take() else {
        return Ok(());
    };
    let PendingBindingFate {
        attached_source_sequence,
        token,
    } = pending;
    match commit.progress_binding_fate_token(token) {
        Ok(token) => {
            slot.binding_fate = Some(PendingBindingFate {
                attached_source_sequence,
                token,
            });
            Ok(())
        }
        Err(token) => {
            slot.binding_fate = Some(PendingBindingFate {
                attached_source_sequence,
                token: *token,
            });
            Err(StateError::invariant(
                "marker ack cursor commit disagrees with sealed binding-fate authority",
            ))
        }
    }
}

fn progress_pending_binding_fate(
    slot: &mut Slot,
    commit: &ParticipantAckCommit,
) -> Result<(), StateError> {
    let Some(pending) = slot.binding_fate.take() else {
        return Ok(());
    };
    let PendingBindingFate {
        attached_source_sequence,
        token,
    } = pending;
    match commit.progress_binding_fate_token(token) {
        Ok(token) => {
            slot.binding_fate = Some(PendingBindingFate {
                attached_source_sequence,
                token,
            });
            Ok(())
        }
        Err(token) => {
            slot.binding_fate = Some(PendingBindingFate {
                attached_source_sequence,
                token: *token,
            });
            Err(StateError::invariant(
                "ack cursor commit disagrees with sealed binding-fate authority",
            ))
        }
    }
}
