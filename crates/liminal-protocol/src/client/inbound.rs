use super::{
    ClientBindingState, ClientParticipantAggregate, ClientResponseCorrelation, correlation,
};
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
    /// Wire identity is insufficient to assign this value to one expected operation.
    AmbiguousResponse,
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

/// Refused body-omitting response with the exact local correlation retained.
#[derive(Debug, PartialEq, Eq)]
pub struct ClientCorrelatedInboundRefusal {
    aggregate: ClientParticipantAggregate,
    value: ServerValue,
    correlation: ClientResponseCorrelation,
    reason: ClientInboundRefusalReason,
}

impl ClientCorrelatedInboundRefusal {
    /// Returns the closed refusal reason.
    #[must_use]
    pub const fn reason(&self) -> ClientInboundRefusalReason {
        self.reason
    }

    /// Releases every unchanged input, including the non-cloneable correlation.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        ClientParticipantAggregate,
        ServerValue,
        ClientResponseCorrelation,
    ) {
        (self.aggregate, self.value, self.correlation)
    }
}

/// Inbound decision for response classes whose wire envelopes omit request identity.
#[derive(Debug, PartialEq, Eq)]
pub enum ClientCorrelatedInboundDecision {
    /// The exact local operation authorization and wire envelope both matched.
    Applied(ClientInboundApplied),
    /// Aggregate, value, and correlation were retained unchanged.
    Refused(ClientCorrelatedInboundRefusal),
}

/// Correlates and applies one server value inside the client aggregate.
#[must_use]
pub fn decide_inbound(
    aggregate: ClientParticipantAggregate,
    value: ServerValue,
) -> ClientInboundDecision {
    decide_inbound_inner(aggregate, value)
}

/// Attempts correlation while retaining the process-local handle on refusal.
///
/// A caller-paired handle is not provenance: `RecordAdmission` remains
/// [`ClientInboundRefusalReason::AmbiguousResponse`] even through this function.
/// `ObserverRecovery` instead uses identity echoed in the wire response. A future
/// SDK leg may upgrade this seam with sealed transport context unavailable to
/// ordinary callers.
#[must_use]
pub fn decide_correlated_inbound(
    aggregate: ClientParticipantAggregate,
    value: ServerValue,
    correlation: ClientResponseCorrelation,
) -> ClientCorrelatedInboundDecision {
    match decide_inbound_inner(aggregate, value) {
        ClientInboundDecision::Applied(applied) => {
            ClientCorrelatedInboundDecision::Applied(applied)
        }
        ClientInboundDecision::Refused(refusal) => {
            let reason = refusal.reason();
            let (aggregate, value) = refusal.into_parts();
            ClientCorrelatedInboundDecision::Refused(ClientCorrelatedInboundRefusal {
                aggregate,
                value,
                correlation,
                reason,
            })
        }
    }
}

fn decide_inbound_inner(
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

    if !aggregate.binding.accepts_request(&expected.request) {
        return inbound_refusal(
            aggregate,
            value,
            ClientInboundRefusalReason::ForeignResponse,
        );
    }

    if matches!(
        expected.request,
        crate::wire::ClientRequest::RecordAdmission(_)
    ) && value.originating_request() == Some(crate::wire::ClientDiscriminant::RecordAdmission)
    {
        return inbound_refusal(
            aggregate,
            value,
            ClientInboundRefusalReason::AmbiguousResponse,
        );
    }

    if !correlation::matches_request(&value, &expected.request) {
        let same_request_class = value.originating_request()
            == Some(expected.request.discriminant())
            || matches!(
                (&value, &expected.request),
                (
                    ServerValue::ObserverRecoveryAccepted(_)
                        | ServerValue::InvalidObserverEpoch(_)
                        | ServerValue::InvalidObserverEpochList(_),
                    crate::wire::ClientRequest::ObserverRecovery(_)
                )
            );
        let reason = if same_request_class && correlation::same_identity(&value, &expected.request)
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
            let attach_secret = match aggregate.binding {
                ClientBindingState::Bound { attach_secret, .. }
                | ClientBindingState::Detached { attach_secret, .. } => attach_secret,
                ClientBindingState::Unbound | ClientBindingState::Left { .. } => return,
            };
            aggregate.binding = ClientBindingState::Detached {
                conversation_id: value.conversation_id(),
                participant_id: value.participant_id(),
                generation: value.capability_generation(),
                attach_secret,
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
        ServerValue::Retired(value) => {
            apply_retired(aggregate, value);
        }
        ServerValue::ParticipantTransportRejected(_)
        | ServerValue::AttemptTokenBodyConflict(_)
        | ServerValue::ConnectionConversationCapacityExceeded(_)
        | ServerValue::ConnectionConversationBindingOccupied(_)
        | ServerValue::ConversationOrderExhausted(_)
        | ServerValue::ParticipantUnknown(_)
        | ServerValue::NoBinding(_)
        | ServerValue::StaleAuthority(_)
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

fn apply_retired(aggregate: &mut ClientParticipantAggregate, value: &crate::wire::Retired) {
    let (conversation_id, participant_id, generation) = match value {
        crate::wire::Retired::Enrollment {
            request,
            participant_id,
            retired_generation,
        } => (
            request.conversation_id,
            *participant_id,
            *retired_generation,
        ),
        crate::wire::Retired::Participant {
            request,
            retired_generation,
        } => {
            let (conversation_id, participant_id) = participant_reference_identity(request);
            (conversation_id, participant_id, *retired_generation)
        }
    };
    aggregate.binding = ClientBindingState::Left {
        conversation_id,
        participant_id,
        generation,
    };
    aggregate
        .detach_replay
        .apply_retired(conversation_id, participant_id, generation);
}

const fn participant_reference_identity(
    request: &crate::wire::ParticipantReferenceEnvelope,
) -> (u64, u64) {
    match request {
        crate::wire::ParticipantReferenceEnvelope::CredentialAttach(value) => {
            (value.conversation_id, value.participant_id)
        }
        crate::wire::ParticipantReferenceEnvelope::Detach(value) => {
            (value.conversation_id, value.participant_id)
        }
        crate::wire::ParticipantReferenceEnvelope::ParticipantAck(value) => {
            (value.conversation_id, value.participant_id)
        }
        crate::wire::ParticipantReferenceEnvelope::Leave(value) => {
            (value.conversation_id, value.participant_id)
        }
        crate::wire::ParticipantReferenceEnvelope::MarkerAck(value) => {
            (value.conversation_id, value.participant_id)
        }
        crate::wire::ParticipantReferenceEnvelope::RecordAdmission(value) => {
            (value.conversation_id, value.participant_id)
        }
    }
}
