//! Obligation-aware nonzero-debt cumulative acknowledgement arm.

use liminal_protocol::lifecycle::{
    AggregateOperationDecision, BindingState, NonzeroParticipantAckCommit,
    NonzeroParticipantAckDecision, PresentedIdentity, RecipientAckObligations,
    SealedBindingFateToken, SemanticConnectionCapacityDecision, apply_nonzero_participant_ack,
    apply_nonzero_participant_ack_frontier, apply_nonzero_participant_ack_with_obligations,
    decide_nonzero_debt_ack_operation, scalar_audit_for_recipient_endpoint,
};
use liminal_protocol::wire::{
    BindingEpoch, ParticipantAck, ParticipantAckEnvelope, ParticipantAckResponse,
    ServerDiscriminant,
};

use crate::server::participant::dispatch_impact::DispatchImpactAccumulator;

use super::barrier::{ArmOutcome, CommitMode, OperationFacts, commit_through_barrier};
use super::facts::Digest;
use super::log::{StoredAck, StoredBindingEpoch, StoredOperation};
use super::observer_progress::ObserverProgressSourceMetadata;
use super::state::{ConversationAuthority, DurableAppend, PendingBindingFate, Slot, StateError};

#[derive(Clone, Copy)]
struct NonzeroCommitContext<'a> {
    receiving_epoch: StoredBindingEpoch,
    scalar_audit: u64,
    operation_facts: &'a OperationFacts,
    appender: &'a dyn DurableAppend,
}

#[derive(Clone, Copy)]
struct NonzeroSelection<'a> {
    identity: PresentedIdentity<'a, Digest, Digest, Digest>,
    binding: &'a BindingState,
    receiving_epoch: BindingEpoch,
    request: &'a ParticipantAck,
    obligations: &'a RecipientAckObligations,
    episode: &'a liminal_protocol::lifecycle::NonzeroDebtCursorEpisode,
    acknowledged_through: u64,
    scalar_audit: u64,
}

pub(super) struct NonzeroAckReplay<'a> {
    pub(super) stored_request: StoredAck,
    pub(super) receiving_epoch: StoredBindingEpoch,
    pub(super) stored_scalar_audit: u64,
    pub(super) ack_obligations: Option<(RecipientAckObligations, u64)>,
    pub(super) event: &'a [u8],
    pub(super) sequence: u64,
}

struct ValidatedNonzeroAckReplay<'a> {
    stored_request: StoredAck,
    receiving_epoch: StoredBindingEpoch,
    stored_scalar_audit: u64,
    obligations: RecipientAckObligations,
    event: &'a [u8],
    sequence: u64,
}

impl<'a> NonzeroAckReplay<'a> {
    fn validate(self) -> Result<ValidatedNonzeroAckReplay<'a>, StateError> {
        let (obligations, reconciled_scalar_audit) = self.ack_obligations.ok_or_else(|| {
            StateError::invariant("nonzero-debt ack replay is missing recipient obligations")
        })?;
        if self.stored_scalar_audit != reconciled_scalar_audit {
            return Err(StateError::invariant(format!(
                "durable nonzero ack scalar audit {} differs from reconciled recipient availability {reconciled_scalar_audit}",
                self.stored_scalar_audit
            )));
        }
        Ok(ValidatedNonzeroAckReplay {
            stored_request: self.stored_request,
            receiving_epoch: self.receiving_epoch,
            stored_scalar_audit: self.stored_scalar_audit,
            obligations,
            event: self.event,
            sequence: self.sequence,
        })
    }
}

impl ConversationAuthority {
    pub(super) fn apply_nonzero_ack_with_impact(
        &mut self,
        request: &ParticipantAck,
        operation_facts: &OperationFacts,
        appender: &dyn DurableAppend,
        impact: &mut DispatchImpactAccumulator,
    ) -> Result<ArmOutcome, StateError> {
        let receiving_epoch = BindingEpoch::new(
            operation_facts.receiving_incarnation,
            request.capability_generation,
        );
        let outbox = self
            .outbox
            .as_ref()
            .ok_or_else(|| StateError::invariant("nonzero ack outbox owner is absent"))?;
        let episode = self
            .obligation_debt_dispatch()
            .and_then(|state| state.episode())
            .ok_or_else(|| StateError::invariant("nonzero ack episode is absent"))?;
        let acknowledged_through = self.slots.get(&request.participant_id).map_or_else(
            || outbox.durable_ack_through(request.participant_id),
            |slot| slot.member.cursor(),
        );
        let (obligations, scalar_audit) =
            outbox.recipient_ack_obligations(request.participant_id, acknowledged_through)?;
        let identity = self
            .slots
            .get(&request.participant_id)
            .map_or(PresentedIdentity::Absent, |slot| {
                PresentedIdentity::<Digest, Digest, Digest>::Live(&slot.member)
            });
        let detached = BindingState::Detached;
        let binding = self
            .slots
            .get(&request.participant_id)
            .map_or(&detached, |slot| &slot.binding);
        let decision = select_conforming_nonzero_ack(NonzeroSelection {
            identity,
            binding,
            receiving_epoch,
            request,
            obligations: &obligations,
            episode,
            acknowledged_through,
            scalar_audit,
        })?;

        match decision {
            NonzeroParticipantAckDecision::Respond(response) => {
                let stage_seven = matches!(
                    response.discriminant(),
                    ServerDiscriminant::AckNoOp
                        | ServerDiscriminant::AckGap
                        | ServerDiscriminant::AckRegression
                );
                if stage_seven {
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
                Ok(ArmOutcome::respond(response.into_server_value()))
            }
            NonzeroParticipantAckDecision::Invariant(error) => Err(StateError::invariant(format!(
                "nonzero ack coupled authority disagrees: {error:?}"
            ))),
            NonzeroParticipantAckDecision::Commit(commit) => self.commit_nonzero_ack(
                request,
                NonzeroCommitContext {
                    receiving_epoch: receiving_epoch.into(),
                    scalar_audit,
                    operation_facts,
                    appender,
                },
                commit,
                impact,
            ),
        }
    }

    pub(super) fn replay_nonzero_debt_ack(
        &mut self,
        replay: NonzeroAckReplay<'_>,
    ) -> Result<(), StateError> {
        let ValidatedNonzeroAckReplay {
            stored_request,
            receiving_epoch,
            stored_scalar_audit,
            obligations,
            event,
            sequence,
        } = replay.validate()?;
        let request = stored_request.to_request()?;
        let receiving = receiving_epoch.to_epoch()?;
        let episode = self
            .obligation_debt_dispatch()
            .and_then(|state| state.episode())
            .ok_or_else(|| StateError::invariant("durable nonzero ack episode is absent"))?;
        let identity = self
            .slots
            .get(&request.participant_id)
            .map_or(PresentedIdentity::Absent, |slot| {
                PresentedIdentity::<Digest, Digest, Digest>::Live(&slot.member)
            });
        let detached = BindingState::Detached;
        let binding = self
            .slots
            .get(&request.participant_id)
            .map_or(&detached, |slot| &slot.binding);
        let acknowledged_through = self
            .slots
            .get(&request.participant_id)
            .map_or(0, |slot| slot.member.cursor());
        let decision = select_conforming_nonzero_ack(NonzeroSelection {
            identity,
            binding,
            receiving_epoch: receiving,
            request: &request,
            obligations: &obligations,
            episode,
            acknowledged_through,
            scalar_audit: stored_scalar_audit,
        })?;
        let NonzeroParticipantAckDecision::Commit(commit) = decision else {
            return Err(StateError::invariant(
                "durable nonzero ack replayed to a non-commit decision",
            ));
        };
        let observer_projection = commit.observer_progress_projection();
        let transitioned =
            apply_nonzero_participant_ack_frontier(self.take_frontier()?, commit.as_ref().clone())
                .map_err(|failure| {
                    StateError::invariant(format!(
                        "durable nonzero ack frontier transition failed: {:?}",
                        failure.error()
                    ))
                })?;
        let (_, frontier) = transitioned.into_parts();
        let shell = self.take_shell()?;
        let barrier = match decide_nonzero_debt_ack_operation(shell, commit) {
            AggregateOperationDecision::Commit(barrier) => barrier,
            AggregateOperationDecision::Refused(refusal) => {
                return Err(StateError::ShellRefused {
                    reason: refusal.reason(),
                });
            }
        };
        let make_operation = |canonical_event| StoredOperation::NonzeroDebtAck {
            request: stored_request,
            receiving_epoch,
            contiguously_available_through: stored_scalar_audit,
            event: canonical_event,
        };
        let (shell, commit) = commit_through_barrier(
            barrier,
            CommitMode::Replay {
                stored_event: event,
                sequence,
            },
            sequence,
            &make_operation,
        )?;
        let slot = self
            .slots
            .get_mut(&request.participant_id)
            .ok_or_else(|| StateError::invariant("durable nonzero ack participant is absent"))?;
        progress_pending_binding_fate(slot, &commit)?;
        self.shell = Some(shell);
        let outcome = self.install_nonzero_ack(frontier, *commit, request.participant_id)?;
        if *outcome.request() != participant_ack_envelope(&request) {
            return Err(StateError::invariant(
                "durable nonzero ack outcome drifted from its stored request",
            ));
        }
        self.advance_log_head()?;
        let metadata = ObserverProgressSourceMetadata::participant_ack(
            sequence,
            request.conversation_id,
            request.participant_id,
            request.through_seq,
        );
        self.record_observer_progress_projection(observer_projection, metadata)?;
        Ok(())
    }

    fn commit_nonzero_ack(
        &mut self,
        request: &ParticipantAck,
        context: NonzeroCommitContext<'_>,
        commit: Box<NonzeroParticipantAckCommit>,
        impact: &mut DispatchImpactAccumulator,
    ) -> Result<ArmOutcome, StateError> {
        let capacity = match context.operation_facts.semantic_connection_capacity() {
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
        let observer_projection = commit.observer_progress_projection();
        let transitioned =
            apply_nonzero_participant_ack_frontier(self.take_frontier()?, commit.as_ref().clone())
                .map_err(|failure| {
                    StateError::invariant(format!(
                        "nonzero ack frontier transition failed: {:?}",
                        failure.error()
                    ))
                })?;
        let (_, frontier) = transitioned.into_parts();
        let shell = self.take_shell()?;
        let barrier = match decide_nonzero_debt_ack_operation(shell, commit) {
            AggregateOperationDecision::Commit(barrier) => barrier,
            AggregateOperationDecision::Refused(refusal) => {
                return Err(StateError::ShellRefused {
                    reason: refusal.reason(),
                });
            }
        };
        let make_operation = |event| StoredOperation::NonzeroDebtAck {
            request: request.into(),
            receiving_epoch: context.receiving_epoch,
            contiguously_available_through: context.scalar_audit,
            event,
        };
        let source_sequence = self.next_log_sequence;
        let (shell, commit) = commit_through_barrier(
            barrier,
            CommitMode::Live(context.appender),
            source_sequence,
            &make_operation,
        )?;
        let slot = self.slots.get_mut(&request.participant_id).ok_or_else(|| {
            StateError::invariant("committed nonzero ack lost its participant slot")
        })?;
        progress_pending_binding_fate(slot, &commit)?;
        self.shell = Some(shell);
        let outcome = self.install_nonzero_ack(frontier, *commit, request.participant_id)?;
        self.advance_log_head()?;
        let metadata = ObserverProgressSourceMetadata::participant_ack(
            source_sequence,
            request.conversation_id,
            request.participant_id,
            request.through_seq,
        );
        self.record_observer_progress_projection(observer_projection, metadata)?;
        self.record_acknowledged(request.participant_id, impact);
        self.record_episode_changed(impact);
        Ok(ArmOutcome::committed(
            ParticipantAckResponse::ack_committed(outcome).into_server_value(),
            capacity,
        ))
    }
}

fn select_conforming_nonzero_ack(
    selection: NonzeroSelection<'_>,
) -> Result<NonzeroParticipantAckDecision, StateError> {
    let obligation_decision = apply_nonzero_participant_ack_with_obligations(
        selection.identity,
        selection.binding,
        selection.receiving_epoch,
        selection.request,
        selection.obligations,
        selection.episode,
    );
    let obligation_only_gap = if let NonzeroParticipantAckDecision::Respond(response) =
        &obligation_decision
        && response.discriminant() == ServerDiscriminant::AckGap
    {
        scalar_audit_for_recipient_endpoint(
            selection.obligations,
            selection.request.participant_id,
            selection.acknowledged_through,
            selection.request.through_seq,
            selection.scalar_audit,
        )
        .map_err(|error| {
            StateError::invariant(format!(
                "nonzero ack scalar audit context disagrees: {error:?}"
            ))
        })?
        .is_none()
    } else {
        false
    };
    if !obligation_only_gap {
        let scalar_decision = apply_nonzero_participant_ack(
            selection.identity,
            selection.binding,
            selection.receiving_epoch,
            selection.request,
            selection.scalar_audit,
            selection.episode,
        );
        if scalar_decision != obligation_decision {
            return Err(StateError::invariant(
                "nonzero ack obligation and scalar selectors diverged",
            ));
        }
    }
    Ok(obligation_decision)
}

fn progress_pending_binding_fate(
    slot: &mut Slot,
    commit: &NonzeroParticipantAckCommit,
) -> Result<(), StateError> {
    let Some(pending) = slot.binding_fate.take() else {
        return Ok(());
    };
    let PendingBindingFate {
        attached_source_sequence,
        token,
    } = pending;
    match progress_fate_token(commit, token) {
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
                "nonzero ack disagrees with sealed binding-fate authority",
            ))
        }
    }
}

fn progress_fate_token(
    commit: &NonzeroParticipantAckCommit,
    token: SealedBindingFateToken,
) -> Result<SealedBindingFateToken, Box<SealedBindingFateToken>> {
    commit.progress_binding_fate_token(token)
}

const fn participant_ack_envelope(request: &ParticipantAck) -> ParticipantAckEnvelope {
    ParticipantAckEnvelope {
        conversation_id: request.conversation_id,
        participant_id: request.participant_id,
        capability_generation: request.capability_generation,
        through_seq: request.through_seq,
    }
}
