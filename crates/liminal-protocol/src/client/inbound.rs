use super::{ClientBindingState, ClientParticipantAggregate, correlation};
use crate::wire::{AttachBound, ReceiptReplay, ServerValue};

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
