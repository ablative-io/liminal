//! Transport-agnostic client participant state and sealed effects.
//!
//! The client aggregate owns correlation, detach replay, and the durability
//! barrier for one outstanding write-ahead operation. Callers persist the
//! pending resume bytes before committing an operation; speculative executable
//! authority is otherwise unreachable.

use crate::wire::{ClientRequest, Generation, ParticipantAckEnvelope};

/// Coarse client binding state without exposing credential-bearing state parts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientBindingStatus {
    /// No participant binding has been established.
    Unbound,
    /// A live binding and attach credential are retained.
    Bound,
    /// The most recently correlated detach completed.
    Detached,
    /// A correlated durable Leave permanently retired the participant.
    Left,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ClientBindingState {
    Unbound,
    Bound {
        conversation_id: u64,
        participant_id: u64,
        generation: Generation,
        attach_secret: crate::wire::AttachSecret,
        binding_epoch: crate::wire::BindingEpoch,
    },
    Detached {
        conversation_id: u64,
        participant_id: u64,
        generation: Generation,
    },
    Left {
        conversation_id: u64,
        participant_id: u64,
        generation: Generation,
    },
}

impl ClientBindingState {
    const fn status(&self) -> ClientBindingStatus {
        match self {
            Self::Unbound => ClientBindingStatus::Unbound,
            Self::Bound { .. } => ClientBindingStatus::Bound,
            Self::Detached { .. } => ClientBindingStatus::Detached,
            Self::Left { .. } => ClientBindingStatus::Left,
        }
    }

    const fn is_left(&self) -> bool {
        matches!(self, Self::Left { .. })
    }

    fn matches_ack(&self, request: &ParticipantAckEnvelope) -> bool {
        match self {
            Self::Bound {
                conversation_id,
                participant_id,
                generation,
                ..
            } => {
                *conversation_id == request.conversation_id
                    && *participant_id == request.participant_id
                    && *generation == request.capability_generation
            }
            Self::Unbound | Self::Detached { .. } | Self::Left { .. } => false,
        }
    }
}

/// Non-cloneable client participant state shell.
///
/// Its expected operation, credential-bearing binding, replay request, and
/// reconnect state are private so callers must delegate every decision.
#[derive(Debug, PartialEq, Eq)]
pub struct ClientParticipantAggregate {
    pub(super) binding: ClientBindingState,
    pub(super) expected: Option<ClientRequest>,
    pub(super) detach_replay: SdkDetachReplayAggregate,
    pub(super) reconnect: ReconnectAggregate,
}

impl ClientParticipantAggregate {
    /// Creates a fresh unbound client aggregate.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            binding: ClientBindingState::Unbound,
            expected: None,
            detach_replay: SdkDetachReplayAggregate::new(),
            reconnect: ReconnectAggregate::new(),
        }
    }

    /// Reports binding status without exposing credential-bearing state.
    #[must_use]
    pub const fn binding_status(&self) -> ClientBindingStatus {
        self.binding.status()
    }

    /// Reports whether one write-ahead operation is outstanding.
    #[must_use]
    pub const fn has_expected_operation(&self) -> bool {
        self.expected.is_some()
    }

    /// Borrows the detach replay aggregate for status inspection.
    #[must_use]
    pub const fn detach_replay(&self) -> &SdkDetachReplayAggregate {
        &self.detach_replay
    }

    /// Borrows the reconnect aggregate for status inspection.
    #[must_use]
    pub const fn reconnect(&self) -> &ReconnectAggregate {
        &self.reconnect
    }
}

impl Default for ClientParticipantAggregate {
    fn default() -> Self {
        Self::new()
    }
}

/// Sealed, non-cloneable authority to execute exactly the committed operation.
#[derive(Debug, PartialEq, Eq)]
pub struct ExpectedParticipantOperation {
    request: ClientRequest,
}

impl ExpectedParticipantOperation {
    /// Borrows the exact request released by the durability barrier.
    #[must_use]
    pub const fn request(&self) -> &ClientRequest {
        &self.request
    }

    /// Consumes the authority into the exact transport-agnostic request.
    #[must_use]
    pub fn into_request(self) -> ClientRequest {
        self.request
    }
}

/// Reason an operation could not enter the write-ahead slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientOperationRecordRefusalReason {
    /// Another write-ahead operation remains outstanding.
    OutstandingOperation,
    /// A different detach replay request remains retained.
    DetachReplayOutstanding,
}

/// Unchanged aggregate and request refused before persistence.
#[derive(Debug, PartialEq, Eq)]
pub struct ClientOperationRecordRefusal {
    aggregate: ClientParticipantAggregate,
    request: ClientRequest,
    reason: ClientOperationRecordRefusalReason,
}

impl ClientOperationRecordRefusal {
    /// Returns the closed refusal reason.
    #[must_use]
    pub const fn reason(&self) -> ClientOperationRecordRefusalReason {
        self.reason
    }

    /// Recovers the unchanged aggregate and refused request.
    #[must_use]
    pub fn into_parts(self) -> (ClientParticipantAggregate, ClientRequest) {
        (self.aggregate, self.request)
    }
}

/// Continuous acknowledgement that bypasses the write-ahead slot.
#[derive(Debug, PartialEq, Eq)]
pub struct ClientContinuousOperation {
    aggregate: ClientParticipantAggregate,
    operation: ExpectedParticipantOperation,
}

impl ClientContinuousOperation {
    /// Releases the unchanged aggregate and executable acknowledgement.
    #[must_use]
    pub fn into_parts(self) -> (ClientParticipantAggregate, ExpectedParticipantOperation) {
        (self.aggregate, self.operation)
    }
}

/// Pending durability decision whose executable parts remain unreachable.
#[derive(Debug, PartialEq, Eq)]
pub struct ClientPendingOperationRecord {
    successor: ClientParticipantAggregate,
    operation: ExpectedParticipantOperation,
    recorded_detach: bool,
}

impl ClientPendingOperationRecord {
    /// Releases the successor aggregate and one-use executable authority after
    /// the caller durably persists the pending resume record.
    #[must_use]
    pub fn commit(self) -> ClientOperationCommit {
        ClientOperationCommit {
            aggregate: self.successor,
            operation: self.operation,
        }
    }

    /// Aborts the speculative successor and returns the unchanged aggregate and
    /// refused request.
    #[must_use]
    pub fn abort(mut self) -> (ClientParticipantAggregate, ClientRequest) {
        self.successor.expected = None;
        if self.recorded_detach {
            self.successor.detach_replay.state = replay::DetachReplayState::Empty;
        }
        (self.successor, self.operation.request)
    }
}

/// Durable operation commit containing the correlated aggregate and execution authority.
#[derive(Debug, PartialEq, Eq)]
pub struct ClientOperationCommit {
    aggregate: ClientParticipantAggregate,
    operation: ExpectedParticipantOperation,
}

impl ClientOperationCommit {
    /// Releases the committed aggregate and one-use expected operation.
    #[must_use]
    pub fn into_parts(self) -> (ClientParticipantAggregate, ExpectedParticipantOperation) {
        (self.aggregate, self.operation)
    }
}

/// Complete write-ahead admission decision.
#[derive(Debug, PartialEq, Eq)]
pub enum ClientOperationRecordDecision {
    /// Durability must precede commit and execution.
    Pending(ClientPendingOperationRecord),
    /// Continuous acknowledgements execute without occupying the slot.
    Continuous(ClientContinuousOperation),
    /// The one permitted slot is already occupied.
    Refused(ClientOperationRecordRefusal),
}

/// Records one operation behind the client durability barrier.
///
/// Continuous acknowledgements bypass the write-ahead slot. Every other request
/// is rejected while an expected operation exists; the crate never queues or
/// silently replaces it.
#[must_use]
pub fn record_operation(
    mut aggregate: ClientParticipantAggregate,
    request: ClientRequest,
) -> ClientOperationRecordDecision {
    if matches!(request, ClientRequest::ParticipantAck(_)) {
        return ClientOperationRecordDecision::Continuous(ClientContinuousOperation {
            aggregate,
            operation: ExpectedParticipantOperation { request },
        });
    }
    if aggregate.expected.is_some() {
        return ClientOperationRecordDecision::Refused(ClientOperationRecordRefusal {
            aggregate,
            request,
            reason: ClientOperationRecordRefusalReason::OutstandingOperation,
        });
    }
    let mut recorded_detach = false;
    if let ClientRequest::Detach(value) = &request {
        let envelope = crate::wire::DetachEnvelope {
            conversation_id: value.conversation_id,
            participant_id: value.participant_id,
            capability_generation: value.capability_generation,
            detach_attempt_token: value.detach_attempt_token,
        };
        match &aggregate.detach_replay.state {
            replay::DetachReplayState::Empty => {
                aggregate.detach_replay.state = replay::DetachReplayState::Recorded {
                    request: envelope,
                    status: DetachReplayStatus::Parked,
                };
                recorded_detach = true;
            }
            replay::DetachReplayState::Recorded {
                request: retained, ..
            } if retained == &envelope => {}
            replay::DetachReplayState::Recorded { .. } => {
                return ClientOperationRecordDecision::Refused(ClientOperationRecordRefusal {
                    aggregate,
                    request,
                    reason: ClientOperationRecordRefusalReason::DetachReplayOutstanding,
                });
            }
        }
    }
    aggregate.expected = Some(request.clone());
    ClientOperationRecordDecision::Pending(ClientPendingOperationRecord {
        successor: aggregate,
        operation: ExpectedParticipantOperation { request },
        recorded_detach,
    })
}

mod correlation;
mod inbound;
mod reconnect;
mod replay;
mod resume;
mod resume_decode;
mod resume_encode;

pub use inbound::{
    ClientInboundApplied, ClientInboundDecision, ClientInboundRefusal, ClientInboundRefusalReason,
    decide_inbound,
};
pub use reconnect::{
    EstablishedConnectionTransportFate, ExplicitReconnectAction, ProvedOnlineTransition,
    ReconnectAggregate, ReconnectAttemptDecision, ReconnectAttemptFate,
    ReconnectAttemptFateDecision, ReconnectAttemptFateRefusalReason, ReconnectAttemptPermit,
    ReconnectAttemptRefusalReason, ReconnectFreshEvent, ReconnectInProgressAttempt,
    ReconnectPermitDecision, ReconnectPermitRefusal, ReconnectPermitRefusalReason,
    RecoveredReconnectPermitDecision, record_attempt_fate, record_explicit_reconnect,
    record_online_transition, record_transport_fate, recover_reconnect_permit, redeem_attempt,
};
pub use replay::{
    ApplyAttachDecision, ApplyDetachOutcomeDecision, ApplyLeaveDecision, DetachReplayApplied,
    DetachReplayOutcome, DetachReplayRefusal, DetachReplayRefusalReason, DetachReplayStatus,
    DetachReplayTerminal, DetachTransportAttempt, DetachTransportAttemptDecision,
    DetachTransportFate, DetachTransportFateDecision, RecordDetachDecision,
    SdkDetachReplayAggregate, apply_attach, apply_detach_outcome, apply_leave_durable,
    record_detach, transport_attempt_started, transport_fate,
};
pub use resume::{
    ClientResumeRecord, ClientResumeRecordDecodeError, ClientResumeRecordEncodeError,
    ClientResumeRecordSection, ClientResumeRestoreError,
};

#[cfg(test)]
mod resume_tests;
#[cfg(test)]
mod tests;
