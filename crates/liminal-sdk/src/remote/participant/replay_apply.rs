use liminal_protocol::client::{
    ApplyAttachDecision, ApplyDetachOutcomeDecision, ApplyLeaveDecision, ClientResponseCorrelation,
    DetachReplayOutcome, DetachReplayRefusalReason, apply_attach, apply_detach_outcome,
    apply_leave_durable,
};
use liminal_protocol::wire::{AttachBound, LeaveCommitted};

use super::{
    ParticipantResumeStore, RemoteParticipantError, RemoteParticipantHandle,
    RemoteReplayApplyOutcome, persist, take_aggregate,
};

impl<S: ParticipantResumeStore> RemoteParticipantHandle<S> {
    /// Delegates an attach supersession input with the held one-use correlation.
    ///
    /// # Errors
    ///
    /// Returns when no response authority exists or persistence fails.
    pub fn apply_attach(
        &self,
        attach: AttachBound,
    ) -> Result<RemoteReplayApplyOutcome<AttachBound>, RemoteParticipantError> {
        self.apply_replay_input(attach, |aggregate, input, correlation| {
            match apply_attach(aggregate, input, correlation) {
                ApplyAttachDecision::Superseded(applied) => (applied.into_aggregate(), None),
                ApplyAttachDecision::Refused(refusal) => {
                    let reason = refusal.reason();
                    let (aggregate, (input, correlation)) = refusal.into_parts();
                    (aggregate, Some((input, correlation, reason)))
                }
            }
        })
    }

    /// Delegates a durable Leave supersession input with held correlation.
    ///
    /// # Errors
    ///
    /// Returns when no response authority exists or persistence fails.
    pub fn apply_leave_durable(
        &self,
        leave: LeaveCommitted,
    ) -> Result<RemoteReplayApplyOutcome<LeaveCommitted>, RemoteParticipantError> {
        self.apply_replay_input(
            leave,
            |aggregate, input, correlation| match apply_leave_durable(aggregate, input, correlation)
            {
                ApplyLeaveDecision::Superseded(applied) => (applied.into_aggregate(), None),
                ApplyLeaveDecision::Refused(refusal) => {
                    let reason = refusal.reason();
                    let (aggregate, (input, correlation)) = refusal.into_parts();
                    (aggregate, Some((input, correlation, reason)))
                }
            },
        )
    }

    /// Delegates one typed terminal detach outcome with held correlation.
    ///
    /// # Errors
    ///
    /// Returns when no response authority exists or persistence fails.
    pub fn apply_detach_outcome(
        &self,
        outcome: DetachReplayOutcome,
    ) -> Result<RemoteReplayApplyOutcome<DetachReplayOutcome>, RemoteParticipantError> {
        self.apply_replay_input(
            outcome,
            |aggregate, input, correlation| match apply_detach_outcome(
                aggregate,
                input,
                correlation,
            ) {
                ApplyDetachOutcomeDecision::Terminal(applied) => (applied.into_aggregate(), None),
                ApplyDetachOutcomeDecision::Refused(refusal) => {
                    let reason = refusal.reason();
                    let (aggregate, (input, correlation)) = refusal.into_parts();
                    (aggregate, Some((input, correlation, reason)))
                }
            },
        )
    }

    fn apply_replay_input<T>(
        &self,
        input: T,
        decide: impl FnOnce(
            liminal_protocol::client::ClientParticipantAggregate,
            T,
            ClientResponseCorrelation,
        ) -> (
            liminal_protocol::client::ClientParticipantAggregate,
            Option<(T, ClientResponseCorrelation, DetachReplayRefusalReason)>,
        ),
    ) -> Result<RemoteReplayApplyOutcome<T>, RemoteParticipantError> {
        let mut state = self.state.lock();
        let correlation = state
            .correlation
            .take()
            .ok_or(RemoteParticipantError::ResponseAuthorityUnavailable)?;
        let aggregate = take_aggregate(&mut state)?;
        let (aggregate, refusal) = decide(aggregate, input, correlation);
        if let Some((input, correlation, reason)) = refusal {
            state.aggregate = Some(aggregate);
            state.correlation = Some(correlation);
            Ok(RemoteReplayApplyOutcome::Refused { input, reason })
        } else {
            persist(&mut state.store, &aggregate)?;
            state.aggregate = Some(aggregate);
            Ok(RemoteReplayApplyOutcome::Applied)
        }
    }
}
