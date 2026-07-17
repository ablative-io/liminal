use liminal_protocol::client::{
    DetachReplayRefusalReason, DetachTransportAttemptDecision, DetachTransportFate,
    DetachTransportFateDecision, ExplicitReconnectAction, LostAuthorityKind,
    LostOperationAuthorityDecision, LostReconnectAuthorityDecision, ProvedOnlineTransition,
    ReconnectAttemptDecision, ReconnectAttemptFate, ReconnectAttemptFateDecision,
    ReconnectAttemptFateRefusalReason, ReconnectAttemptRefusalReason, ReconnectPermitDecision,
    RecoveredExpectedOperationDecision, RecoveredReconnectPermitDecision, record_attempt_fate,
    record_explicit_reconnect, record_online_transition, recover_expected_operation,
    recover_reconnect_permit, redeem_attempt, resolve_lost_operation_authority,
    resolve_lost_reconnect_authority, transport_attempt_started, transport_fate,
};
use liminal_protocol::outcome::ReconnectState;
use liminal_protocol::wire::{ClientRequest, DetachRequest};

use super::{
    OperationDurability, ParticipantResumeStore, RemoteOperationTransportFate,
    RemoteParticipantError, RemoteParticipantHandle, RemoteParticipantOperation,
    RemoteParticipantSendOutcome, RemoteReconnectPermit, RemoteReconnectPermitOutcome, persist,
    record_connection_fate, record_operation_transport_fate, take_aggregate,
};

/// Result of releasing a committed cold-restored operation.
#[derive(Debug)]
pub enum RemoteExpectedOperationRecovery {
    /// One unissued operation authority was recovered.
    Recovered(RemoteParticipantOperation),
    /// No recoverable operation exists.
    NotAvailable {
        /// Whether the retained operation had already been issued.
        already_issued: bool,
    },
}

/// Result of consuming operation-domain crash testimony.
#[derive(Debug, PartialEq, Eq)]
pub enum RemoteLostOperationResolution {
    /// A non-detach operation was terminalized by serialized testimony.
    Recorded {
        /// Exact request whose authority was destroyed.
        request: ClientRequest,
        /// Closed testimony kind consumed by the crate.
        testimony: LostAuthorityKind,
    },
    /// A detach was returned to parked replay by serialized testimony.
    DetachParked {
        /// Exact detach request retained for replay.
        request: ClientRequest,
        /// Closed testimony kind consumed by the crate.
        testimony: LostAuthorityKind,
    },
    /// No operation-domain testimony was pending.
    Refused {
        /// Closed crate refusal reason.
        reason: liminal_protocol::client::LostAuthorityResolutionRefusalReason,
    },
}

/// Result of consuming reconnect-domain crash testimony.
#[derive(Debug, PartialEq, Eq)]
pub enum RemoteLostReconnectResolution {
    /// Testimony parked reconnect state without minting replacement authority.
    Recorded {
        /// Closed testimony kind consumed by the crate.
        testimony: LostAuthorityKind,
    },
    /// No reconnect-domain testimony was pending.
    Refused {
        /// Closed crate refusal reason.
        reason: liminal_protocol::client::LostAuthorityResolutionRefusalReason,
    },
}

/// Result of releasing a committed cold-restored reconnect permit.
#[derive(Debug)]
pub enum RemoteReconnectPermitRecovery {
    /// One unissued permit was recovered.
    Recovered(RemoteReconnectPermit),
    /// No recoverable permit exists.
    NotAvailable {
        /// Current crate reconnect state.
        state: ReconnectState,
    },
}

/// Result of a real connection attempt redeemed from one crate permit.
#[derive(Debug)]
pub enum RemoteReconnectAttemptOutcome {
    /// The real attempt connected and the crate recorded online state.
    Connected {
        /// Provenance assigned to the new established socket.
        provenance: super::ParticipantResponseProvenance,
    },
    /// The real attempt failed and the crate parked without timer authority.
    Failed {
        /// Concrete socket failure.
        error: crate::SdkError,
    },
    /// The crate refused permit redemption and returned it unchanged.
    Refused {
        /// Reusable unchanged permit.
        permit: RemoteReconnectPermit,
        /// Closed crate refusal reason.
        reason: ReconnectAttemptRefusalReason,
    },
    /// The transport ran, but the crate retained the in-progress fate authority.
    FateRefused {
        /// Closed crate refusal reason.
        reason: ReconnectAttemptFateRefusalReason,
        /// Socket failure when the attempted fate was `Failed`.
        error: Option<crate::SdkError>,
        /// New socket provenance when the attempted fate was `Connected`.
        provenance: Option<super::ParticipantResponseProvenance>,
    },
}

/// Result of starting and sending a parked detach replay.
#[derive(Debug)]
pub enum RemoteDetachReplayOutcome {
    /// A real transport send was attempted.
    Send(RemoteParticipantSendOutcome),
    /// The crate refused replay start without changing state.
    Refused {
        /// Closed crate replay refusal reason.
        reason: DetachReplayRefusalReason,
    },
}

/// Result of an explicit replay apply seam delegated to the crate.
#[derive(Debug, PartialEq, Eq)]
pub enum RemoteReplayApplyOutcome<T> {
    /// The crate applied the transition.
    Applied,
    /// The crate retained state, correlation, and exact input.
    Refused {
        /// Exact refused input.
        input: T,
        /// Closed crate refusal reason.
        reason: DetachReplayRefusalReason,
    },
}

/// Combined typed consequence of an established connection loss.
#[derive(Debug)]
pub struct RemoteTransportLossOutcome {
    /// Operation-domain fate selected by crate rules.
    pub operation_fate: RemoteOperationTransportFate,
    /// Event-driven reconnect decision selected by crate rules.
    pub reconnect: RemoteReconnectPermitOutcome,
}

impl<S: ParticipantResumeStore> RemoteParticipantHandle<S> {
    /// Releases one unissued operation from committed cold-restored state.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteParticipantError::StateUnavailable`] after a prior fatal
    /// durability failure.
    pub fn recover_expected_operation(
        &self,
    ) -> Result<RemoteExpectedOperationRecovery, RemoteParticipantError> {
        let mut state = self.state.lock();
        let aggregate = take_aggregate(&mut state)?;
        match recover_expected_operation(aggregate) {
            RecoveredExpectedOperationDecision::Recovered {
                aggregate,
                operation,
            } => {
                state.aggregate = Some(aggregate);
                Ok(RemoteExpectedOperationRecovery::Recovered(
                    RemoteParticipantOperation {
                        operation,
                        durability: OperationDurability::WriteAhead,
                    },
                ))
            }
            RecoveredExpectedOperationDecision::NotAvailable {
                aggregate,
                already_issued,
            } => {
                state.aggregate = Some(aggregate);
                Ok(RemoteExpectedOperationRecovery::NotAvailable { already_issued })
            }
        }
    }

    /// Consumes operation-domain lost-authority testimony exactly once.
    ///
    /// # Errors
    ///
    /// Returns LPCR encode or storage failures while checkpointing the decision.
    pub fn resolve_lost_operation_authority(
        &self,
    ) -> Result<RemoteLostOperationResolution, RemoteParticipantError> {
        let mut state = self.state.lock();
        let aggregate = take_aggregate(&mut state)?;
        let outcome = match resolve_lost_operation_authority(aggregate) {
            LostOperationAuthorityDecision::Recorded {
                aggregate,
                request,
                testimony,
            } => {
                state.aggregate = Some(aggregate);
                RemoteLostOperationResolution::Recorded {
                    request,
                    testimony: testimony.kind(),
                }
            }
            LostOperationAuthorityDecision::DetachParked {
                aggregate,
                request,
                testimony,
            } => {
                state.aggregate = Some(aggregate);
                RemoteLostOperationResolution::DetachParked {
                    request,
                    testimony: testimony.kind(),
                }
            }
            LostOperationAuthorityDecision::Refused { aggregate, reason } => {
                state.aggregate = Some(aggregate);
                RemoteLostOperationResolution::Refused { reason }
            }
        };
        checkpoint_state(&mut state)?;
        Ok(outcome)
    }

    /// Takes a durable tokenless abandonment so its exact request can be re-recorded.
    ///
    /// # Errors
    ///
    /// Returns LPCR encode or storage failures while durably recording the take.
    pub fn take_restored_operation_abandonment(
        &self,
    ) -> Result<
        Option<liminal_protocol::client::RestoredExpectedOperationAbandonment>,
        RemoteParticipantError,
    > {
        let mut state = self.state.lock();
        let mut aggregate = take_aggregate(&mut state)?;
        let abandonment = aggregate.take_restored_operation_abandonment();
        if abandonment.is_some() {
            persist(&mut state.store, &aggregate)?;
        }
        state.aggregate = Some(aggregate);
        Ok(abandonment)
    }

    /// Records established-connection fate and returns at most one reconnect permit.
    ///
    /// # Errors
    ///
    /// Returns LPCR encode or storage failures while checkpointing the event.
    pub fn record_transport_fate(
        &self,
    ) -> Result<RemoteReconnectPermitOutcome, RemoteParticipantError> {
        let mut state = self.state.lock();
        record_connection_fate(&mut state)
    }

    /// Records a proved online transition as a crate fresh event.
    ///
    /// # Errors
    ///
    /// Returns LPCR encode or storage failures while checkpointing issued authority.
    pub fn record_online_transition(
        &self,
    ) -> Result<RemoteReconnectPermitOutcome, RemoteParticipantError> {
        self.record_fresh_reconnect(|aggregate| {
            record_online_transition(aggregate, ProvedOnlineTransition::ProvedOnline)
        })
    }

    /// Records explicit caller action as a crate fresh event, with no timer arm.
    ///
    /// # Errors
    ///
    /// Returns LPCR encode or storage failures while checkpointing issued authority.
    pub fn record_explicit_reconnect(
        &self,
    ) -> Result<RemoteReconnectPermitOutcome, RemoteParticipantError> {
        self.record_fresh_reconnect(|aggregate| {
            record_explicit_reconnect(aggregate, ExplicitReconnectAction::ReconnectNow)
        })
    }

    fn record_fresh_reconnect(
        &self,
        decide: impl FnOnce(
            liminal_protocol::client::ClientParticipantAggregate,
        ) -> ReconnectPermitDecision,
    ) -> Result<RemoteReconnectPermitOutcome, RemoteParticipantError> {
        let mut state = self.state.lock();
        let aggregate = take_aggregate(&mut state)?;
        let outcome = match decide(aggregate) {
            ReconnectPermitDecision::Permitted {
                aggregate,
                permit,
                result,
            } => {
                state.aggregate = Some(aggregate);
                RemoteReconnectPermitOutcome::Permitted {
                    permit: RemoteReconnectPermit { permit },
                    result,
                }
            }
            ReconnectPermitDecision::Refused(refusal) => {
                let reason = refusal.reason();
                let result = refusal.result();
                let (aggregate, _) = refusal.into_parts();
                state.aggregate = Some(aggregate);
                RemoteReconnectPermitOutcome::Refused { reason, result }
            }
        };
        checkpoint_state(&mut state)?;
        Ok(outcome)
    }

    /// Releases one unissued reconnect permit from committed cold-restored state.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteParticipantError::StateUnavailable`] after a prior fatal failure.
    pub fn recover_reconnect_permit(
        &self,
    ) -> Result<RemoteReconnectPermitRecovery, RemoteParticipantError> {
        let mut state = self.state.lock();
        let aggregate = take_aggregate(&mut state)?;
        match recover_reconnect_permit(aggregate) {
            RecoveredReconnectPermitDecision::Recovered { aggregate, permit } => {
                state.aggregate = Some(aggregate);
                Ok(RemoteReconnectPermitRecovery::Recovered(
                    RemoteReconnectPermit { permit },
                ))
            }
            RecoveredReconnectPermitDecision::NotAvailable {
                aggregate,
                state: value,
            } => {
                state.aggregate = Some(aggregate);
                Ok(RemoteReconnectPermitRecovery::NotAvailable { state: value })
            }
        }
    }

    /// Consumes reconnect-domain lost-authority testimony exactly once.
    ///
    /// # Errors
    ///
    /// Returns LPCR encode or storage failures while checkpointing the resolution.
    pub fn resolve_lost_reconnect_authority(
        &self,
    ) -> Result<RemoteLostReconnectResolution, RemoteParticipantError> {
        let mut state = self.state.lock();
        let aggregate = take_aggregate(&mut state)?;
        let outcome = match resolve_lost_reconnect_authority(aggregate) {
            LostReconnectAuthorityDecision::Recorded {
                aggregate,
                testimony,
            } => {
                state.aggregate = Some(aggregate);
                RemoteLostReconnectResolution::Recorded {
                    testimony: testimony.kind(),
                }
            }
            LostReconnectAuthorityDecision::Refused { aggregate, reason } => {
                state.aggregate = Some(aggregate);
                RemoteLostReconnectResolution::Refused { reason }
            }
        };
        checkpoint_state(&mut state)?;
        Ok(outcome)
    }

    /// Redeems one permit before opening one real transport connection.
    ///
    /// # Errors
    ///
    /// Returns LPCR encode or storage failures before or after the real attempt.
    pub fn reconnect(
        &self,
        permit: RemoteReconnectPermit,
    ) -> Result<RemoteReconnectAttemptOutcome, RemoteParticipantError> {
        let mut state = self.state.lock();
        let aggregate = take_aggregate(&mut state)?;
        let (aggregate, attempt) = match redeem_attempt(aggregate, permit.permit) {
            ReconnectAttemptDecision::Started { aggregate, attempt } => (aggregate, attempt),
            ReconnectAttemptDecision::Refused {
                aggregate,
                permit,
                reason,
            } => {
                state.aggregate = Some(aggregate);
                return Ok(RemoteReconnectAttemptOutcome::Refused {
                    permit: RemoteReconnectPermit { permit },
                    reason,
                });
            }
        };
        persist(&mut state.store, &aggregate)?;
        state.aggregate = Some(aggregate);
        drop(state);

        let transport_result = self.transport.reconnect_participant(&self.server_address);
        let fate = if transport_result.is_ok() {
            ReconnectAttemptFate::Connected
        } else {
            ReconnectAttemptFate::Failed
        };

        let mut state = self.state.lock();
        let aggregate = take_aggregate(&mut state)?;
        match record_attempt_fate(aggregate, attempt, fate) {
            ReconnectAttemptFateDecision::Recorded(aggregate) => {
                persist(&mut state.store, &aggregate)?;
                state.aggregate = Some(aggregate);
                match transport_result {
                    Ok(provenance) => Ok(RemoteReconnectAttemptOutcome::Connected { provenance }),
                    Err(error) => Ok(RemoteReconnectAttemptOutcome::Failed { error }),
                }
            }
            ReconnectAttemptFateDecision::Refused {
                aggregate,
                attempt,
                reason,
                ..
            } => {
                state.aggregate = Some(aggregate);
                state.reconnect_attempt = Some(attempt);
                let (provenance, error) = match transport_result {
                    Ok(value) => (Some(value), None),
                    Err(value) => (None, Some(value)),
                };
                Ok(RemoteReconnectAttemptOutcome::FateRefused {
                    reason,
                    error,
                    provenance,
                })
            }
        }
    }

    /// Records response and connection fates after an established transport loss.
    ///
    /// # Errors
    ///
    /// Returns LPCR encode or storage failures while checkpointing both decisions.
    pub fn record_established_transport_loss(
        &self,
    ) -> Result<RemoteTransportLossOutcome, RemoteParticipantError> {
        let mut state = self.state.lock();
        let operation_fate = if let Some(correlation) = state.correlation.take() {
            let aggregate = take_aggregate(&mut state)?;
            record_operation_transport_fate(&mut state, aggregate, correlation)
        } else {
            RemoteOperationTransportFate::NotOutstanding
        };
        let reconnect = record_connection_fate(&mut state)?;
        Ok(RemoteTransportLossOutcome {
            operation_fate,
            reconnect,
        })
    }

    /// Starts and sends the exact parked detach replay selected by the crate.
    ///
    /// # Errors
    ///
    /// Returns LPCR, storage, or state failures. Socket failure is a typed send outcome.
    pub fn replay_detach(&self) -> Result<RemoteDetachReplayOutcome, RemoteParticipantError> {
        let mut state = self.state.lock();
        let aggregate = take_aggregate(&mut state)?;
        let (aggregate, attempt) = match transport_attempt_started(aggregate) {
            DetachTransportAttemptDecision::Started { aggregate, attempt } => (aggregate, attempt),
            DetachTransportAttemptDecision::Refused(refusal) => {
                let reason = refusal.reason();
                let (aggregate, ()) = refusal.into_parts();
                state.aggregate = Some(aggregate);
                return Ok(RemoteDetachReplayOutcome::Refused { reason });
            }
        };
        persist(&mut state.store, &aggregate)?;
        let (request, correlation) = attempt.into_request();
        let request = ClientRequest::Detach(DetachRequest {
            conversation_id: request.conversation_id,
            participant_id: request.participant_id,
            capability_generation: request.capability_generation,
            detach_attempt_token: request.detach_attempt_token,
        });
        match self
            .transport
            .send_participant(&self.server_address, &request)
        {
            Ok(provenance) => {
                state.aggregate = Some(aggregate);
                state.correlation = Some(correlation);
                Ok(RemoteDetachReplayOutcome::Send(
                    RemoteParticipantSendOutcome::Sent { provenance },
                ))
            }
            Err(error) => {
                let operation_fate = match transport_fate(
                    aggregate,
                    correlation,
                    DetachTransportFate::ResponseUnavailable,
                ) {
                    DetachTransportFateDecision::Parked(applied) => {
                        state.aggregate = Some(applied.into_aggregate());
                        RemoteOperationTransportFate::DetachParked
                    }
                    DetachTransportFateDecision::Refused(refusal) => {
                        let (aggregate, (correlation, _)) = refusal.into_parts();
                        state.aggregate = Some(aggregate);
                        state.correlation = Some(correlation);
                        RemoteOperationTransportFate::Refused {
                            reason: liminal_protocol::client::ExpectedOperationFateRefusalReason::DetachUsesReplayFate,
                        }
                    }
                };
                let reconnect = record_connection_fate(&mut state)?;
                Ok(RemoteDetachReplayOutcome::Send(
                    RemoteParticipantSendOutcome::TransportLost {
                        error,
                        operation_fate,
                        reconnect,
                    },
                ))
            }
        }
    }
}

fn checkpoint_state<S: ParticipantResumeStore>(
    state: &mut super::RemoteParticipantState<S>,
) -> Result<(), RemoteParticipantError> {
    let aggregate = take_aggregate(state)?;
    persist(&mut state.store, &aggregate)?;
    state.aggregate = Some(aggregate);
    Ok(())
}
