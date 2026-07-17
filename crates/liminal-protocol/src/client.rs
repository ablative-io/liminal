//! Transport-agnostic client participant state and sealed effects.
//!
//! The client aggregate owns correlation, detach replay, and the durability
//! barrier for one outstanding write-ahead operation. Callers persist the
//! pending resume bytes before committing an operation; speculative executable
//! authority is otherwise unreachable.

use crate::wire::{
    AttachBound, ClientRequest, Generation, ParticipantAckEnvelope, ReceiptReplay, ServerValue,
};

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
}

impl ClientParticipantAggregate {
    /// Creates a fresh unbound client aggregate.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            binding: ClientBindingState::Unbound,
            expected: None,
            detach_replay: SdkDetachReplayAggregate::new(),
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
    aggregate.expected = Some(request.clone());
    ClientOperationRecordDecision::Pending(ClientPendingOperationRecord {
        successor: aggregate,
        operation: ExpectedParticipantOperation { request },
    })
}

/// Closed refusal classes for inbound semantic values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientInboundRefusalReason {
    /// A durable Leave already terminalized the local participant.
    AlreadyDead,
    /// The value names another operation or participant identity.
    ForeignResponse,
    /// The value is absent an expectation or belongs to an older request.
    DelayedResponse,
}

/// Applied inbound value and resulting aggregate.
#[derive(Debug, PartialEq, Eq)]
pub struct ClientInboundApplied {
    aggregate: ClientParticipantAggregate,
    value: ServerValue,
}

impl ClientInboundApplied {
    /// Releases the resulting aggregate and exact applied value.
    #[must_use]
    pub fn into_parts(self) -> (ClientParticipantAggregate, ServerValue) {
        (self.aggregate, self.value)
    }
}

/// Refused inbound value paired with the unchanged aggregate.
#[derive(Debug, PartialEq, Eq)]
pub struct ClientInboundRefusal {
    aggregate: ClientParticipantAggregate,
    value: ServerValue,
    reason: ClientInboundRefusalReason,
}

impl ClientInboundRefusal {
    /// Returns the closed refusal reason.
    #[must_use]
    pub const fn reason(&self) -> ClientInboundRefusalReason {
        self.reason
    }

    /// Releases the unchanged aggregate and exact refused value.
    #[must_use]
    pub fn into_parts(self) -> (ClientParticipantAggregate, ServerValue) {
        (self.aggregate, self.value)
    }
}

/// Exhaustive inbound correlation decision.
#[derive(Debug, PartialEq, Eq)]
pub enum ClientInboundDecision {
    /// The crate correlated and applied the typed value.
    Applied(ClientInboundApplied),
    /// The crate retained both authority and value unchanged.
    Refused(ClientInboundRefusal),
}

/// Correlates and applies one server value inside the client aggregate.
#[must_use]
pub fn decide_inbound(
    mut aggregate: ClientParticipantAggregate,
    value: ServerValue,
) -> ClientInboundDecision {
    if aggregate.binding.is_left() {
        return inbound_refusal(aggregate, value, ClientInboundRefusalReason::AlreadyDead);
    }

    if let Some(request) = correlation::participant_ack_request(&value) {
        if aggregate.binding.matches_ack(request) {
            return ClientInboundDecision::Applied(ClientInboundApplied { aggregate, value });
        }
        return inbound_refusal(
            aggregate,
            value,
            ClientInboundRefusalReason::ForeignResponse,
        );
    }

    if matches!(value, ServerValue::ParticipantTransportRejected(_)) {
        return ClientInboundDecision::Applied(ClientInboundApplied { aggregate, value });
    }

    let Some(expected) = aggregate.expected.as_ref() else {
        return inbound_refusal(
            aggregate,
            value,
            ClientInboundRefusalReason::DelayedResponse,
        );
    };

    if !correlation::matches_request(&value, expected) {
        let reason = if value.originating_request() == Some(expected.discriminant())
            && correlation::same_identity(&value, expected)
        {
            ClientInboundRefusalReason::DelayedResponse
        } else {
            ClientInboundRefusalReason::ForeignResponse
        };
        return inbound_refusal(aggregate, value, reason);
    }

    aggregate.expected = None;
    apply_correlated_value(&mut aggregate, &value);
    ClientInboundDecision::Applied(ClientInboundApplied { aggregate, value })
}

const fn inbound_refusal(
    aggregate: ClientParticipantAggregate,
    value: ServerValue,
    reason: ClientInboundRefusalReason,
) -> ClientInboundDecision {
    ClientInboundDecision::Refused(ClientInboundRefusal {
        aggregate,
        value,
        reason,
    })
}

fn apply_correlated_value(aggregate: &mut ClientParticipantAggregate, value: &ServerValue) {
    match value {
        ServerValue::EnrollBound(value) => apply_enroll_bound(aggregate, value),
        ServerValue::Bound(ReceiptReplay::Enrollment(value)) => {
            apply_enroll_bound(aggregate, value);
        }
        ServerValue::AttachBound(value)
        | ServerValue::Bound(ReceiptReplay::CredentialAttach(value)) => {
            apply_attach_bound(aggregate, value);
            aggregate.detach_replay.apply_attach(value);
        }
        ServerValue::DetachCommitted(value) => {
            aggregate.binding = ClientBindingState::Detached {
                conversation_id: value.conversation_id(),
                participant_id: value.participant_id(),
                generation: value.capability_generation(),
            };
            aggregate.detach_replay.apply_detach_committed(value);
        }
        ServerValue::DetachInProgress(value) => {
            aggregate.detach_replay.apply_detach_in_progress(value);
        }
        ServerValue::StaleAuthority(crate::wire::StaleAuthority::Detach(
            crate::wire::DetachStaleAuthority::TerminalizedDetachCell(value),
        )) => {
            aggregate
                .detach_replay
                .apply_terminalized_detach_cell(value);
        }
        ServerValue::LeaveCommitted(value) => {
            aggregate.binding = ClientBindingState::Left {
                conversation_id: value.conversation_id(),
                participant_id: value.participant_id(),
                generation: value.retired_generation(),
            };
            aggregate.detach_replay.apply_leave(value);
        }
        ServerValue::ParticipantTransportRejected(_)
        | ServerValue::AttemptTokenBodyConflict(_)
        | ServerValue::ConnectionConversationCapacityExceeded(_)
        | ServerValue::ConnectionConversationBindingOccupied(_)
        | ServerValue::ConversationOrderExhausted(_)
        | ServerValue::ParticipantUnknown(_)
        | ServerValue::NoBinding(_)
        | ServerValue::StaleAuthority(_)
        | ServerValue::Retired(_)
        | ServerValue::MarkerClosureCapacityExceeded(_)
        | ServerValue::EnrollmentKnown(_)
        | ServerValue::ReceiptExpired(_)
        | ServerValue::ReceiptCapacityExceeded(_)
        | ServerValue::IdentityCapacityExceeded(_)
        | ServerValue::ObserverBackpressure(_)
        | ServerValue::ConversationSequenceExhausted(_)
        | ServerValue::StaleOrUnknownReceipt(_)
        | ServerValue::MarkerNotDelivered(_)
        | ServerValue::MarkerMismatch(_)
        | ServerValue::UnboundReceipt(_)
        | ServerValue::AckCommitted(_)
        | ServerValue::AckNoOp(_)
        | ServerValue::AckGap(_)
        | ServerValue::AckRegression(_)
        | ServerValue::MarkerAckCommitted(_)
        | ServerValue::RecordCommitted(_)
        | ServerValue::RecordTooLarge(_)
        | ServerValue::ObserverRecoveryAccepted(_)
        | ServerValue::InvalidObserverEpoch(_)
        | ServerValue::InvalidObserverEpochList(_) => {}
    }
}

const fn apply_enroll_bound(
    aggregate: &mut ClientParticipantAggregate,
    value: &crate::wire::EnrollBound,
) {
    aggregate.binding = ClientBindingState::Bound {
        conversation_id: value.conversation_id(),
        participant_id: value.participant_id(),
        generation: value.capability_generation(),
        attach_secret: value.attach_secret(),
        binding_epoch: value.origin_binding_epoch(),
    };
}

const fn apply_attach_bound(aggregate: &mut ClientParticipantAggregate, value: &AttachBound) {
    aggregate.binding = ClientBindingState::Bound {
        conversation_id: value.conversation_id(),
        participant_id: value.participant_id(),
        generation: value.capability_generation(),
        attach_secret: value.attach_secret(),
        binding_epoch: value.origin_binding_epoch(),
    };
}

mod correlation;
mod replay;

pub use replay::{
    ApplyAttachDecision, ApplyDetachOutcomeDecision, ApplyLeaveDecision, DetachReplayApplied,
    DetachReplayOutcome, DetachReplayRefusal, DetachReplayRefusalReason, DetachReplayStatus,
    DetachReplayTerminal, DetachTransportAttempt, DetachTransportAttemptDecision,
    DetachTransportFate, DetachTransportFateDecision, RecordDetachDecision,
    SdkDetachReplayAggregate, apply_attach, apply_detach_outcome, apply_leave_durable,
    record_detach, transport_attempt_started, transport_fate,
};

#[cfg(test)]
mod tests;
