use crate::wire::{
    AckNoOp, AttachEnvelope, BindingRequiredEnvelope, ClientRequest, ClosureCheckedEnvelope,
    CommonStaleAuthorityEnvelope, DetachEnvelope, DetachStaleAuthority, EnrollmentEnvelope,
    Generation, LeaveEnvelope, LeaveStaleAuthority, MarkerAckEnvelope, MarkerProofRequest,
    ObserverBackpressure, OrderAllocatingEnvelope, ParticipantAckEnvelope,
    ParticipantReferenceEnvelope, ReceiptExpired, ReceiptReplay, RecordAdmissionEnvelope,
    ResponseEnvelope, SequenceAllocatingEnvelope, ServerValue, StaleAuthority,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RequestKey {
    Unsolicited,
    ObserverRecovery(u64),
    Enrollment(u64, crate::wire::EnrollmentToken),
    Attach(
        u64,
        u64,
        Generation,
        crate::wire::AttachAttemptToken,
        Option<u64>,
    ),
    Detach(u64, u64, Generation, crate::wire::DetachAttemptToken),
    Ack(u64, u64, Generation, u64),
    Leave(u64, u64, Generation, crate::wire::LeaveAttemptToken),
    MarkerAck(u64, u64, Generation, u64),
    Record(u64, u64, Generation, u64),
}

impl RequestKey {
    const fn from_request(request: &ClientRequest, authorization: u64) -> Self {
        match request {
            ClientRequest::Enrollment(value) => {
                Self::Enrollment(value.conversation_id, value.enrollment_token)
            }
            ClientRequest::CredentialAttach(value) => Self::Attach(
                value.conversation_id,
                value.participant_id,
                value.capability_generation,
                value.attach_attempt_token,
                value.accept_marker_delivery_seq,
            ),
            ClientRequest::Detach(value) => Self::Detach(
                value.conversation_id,
                value.participant_id,
                value.capability_generation,
                value.detach_attempt_token,
            ),
            ClientRequest::ParticipantAck(value) => Self::Ack(
                value.conversation_id,
                value.participant_id,
                value.capability_generation,
                value.through_seq,
            ),
            ClientRequest::Leave(value) => Self::Leave(
                value.conversation_id,
                value.participant_id,
                value.capability_generation,
                value.leave_attempt_token,
            ),
            ClientRequest::MarkerAck(value) => Self::MarkerAck(
                value.conversation_id,
                value.participant_id,
                value.capability_generation,
                value.marker_delivery_seq,
            ),
            ClientRequest::RecordAdmission(value) => Self::Record(
                value.conversation_id,
                value.participant_id,
                value.capability_generation,
                authorization,
            ),
            ClientRequest::ObserverRecovery(_) => Self::ObserverRecovery(authorization),
        }
    }

    const fn same_identity(self, other: Self) -> bool {
        match (self, other) {
            (Self::Enrollment(left, _), Self::Enrollment(right, _)) => left == right,
            (Self::Attach(lc, lp, ..), Self::Attach(rc, rp, ..))
            | (Self::Detach(lc, lp, ..), Self::Detach(rc, rp, ..))
            | (Self::Ack(lc, lp, ..), Self::Ack(rc, rp, ..))
            | (Self::Leave(lc, lp, ..), Self::Leave(rc, rp, ..))
            | (Self::MarkerAck(lc, lp, ..), Self::MarkerAck(rc, rp, ..))
            | (Self::Record(lc, lp, ..), Self::Record(rc, rp, ..)) => lc == rc && lp == rp,
            (Self::ObserverRecovery(_), Self::ObserverRecovery(_)) => true,
            _ => false,
        }
    }
}

/// Matches only response identity carried by the wire itself.
///
/// `RecordAdmission` deliberately has no echoed payload/token and therefore
/// never matches here. A future SDK integration may supply a sealed transport
/// context, but no caller-pairable value is accepted as provenance in this leg.
pub(super) fn matches_request(value: &ServerValue, request: &ClientRequest) -> bool {
    match request {
        ClientRequest::RecordAdmission(_) => false,
        ClientRequest::ObserverRecovery(expected) => observer_response_matches(value, expected),
        _ => response_key(value, 0) == RequestKey::from_request(request, 0),
    }
}

fn observer_response_matches(
    value: &ServerValue,
    expected: &crate::wire::ObserverRecoveryHandshake,
) -> bool {
    match value {
        ServerValue::ObserverRecoveryAccepted(response) => {
            response.statuses.len() == expected.observer_refusals.len()
                && response
                    .statuses
                    .iter()
                    .zip(&expected.observer_refusals)
                    .all(|(actual, expected)| {
                        actual.conversation_id == expected.conversation_id
                            && actual.refused_epoch == expected.refused_epoch
                    })
        }
        ServerValue::InvalidObserverEpoch(response) if expected.observer_refusals.len() == 1 => {
            let Some(expected) = expected.observer_refusals.first() else {
                return false;
            };
            match response {
                crate::wire::InvalidObserverEpoch::ConversationUnknown {
                    conversation_id,
                    presented_epoch,
                }
                | crate::wire::InvalidObserverEpoch::EpochAhead {
                    conversation_id,
                    presented_epoch,
                    ..
                } => {
                    *conversation_id == expected.conversation_id
                        && *presented_epoch == expected.refused_epoch
                }
            }
        }
        _ => false,
    }
}

pub(super) fn same_identity(value: &ServerValue, request: &ClientRequest) -> bool {
    response_key(value, 0).same_identity(RequestKey::from_request(request, 0))
}

pub(super) const fn participant_ack_request(
    value: &ServerValue,
) -> Option<&ParticipantAckEnvelope> {
    match value {
        ServerValue::AckCommitted(value) => Some(value.request()),
        ServerValue::AckNoOp(AckNoOp::ParticipantAck(value)) => Some(value),
        ServerValue::AckGap(value) => Some(value.request()),
        ServerValue::AckRegression(value) => Some(value.request()),
        _ => None,
    }
}

fn response_key(value: &ServerValue, ambiguous_authorization: u64) -> RequestKey {
    let key = match value {
        ServerValue::ParticipantTransportRejected(_) => RequestKey::Unsolicited,
        ServerValue::AttemptTokenBodyConflict(value) => attempt_conflict(value),
        ServerValue::ConnectionConversationCapacityExceeded(value) => connection_capacity(value),
        ServerValue::ConnectionConversationBindingOccupied(value) => binding_occupied(value),
        ServerValue::ConversationOrderExhausted(value) => order_envelope(value.request()),
        ServerValue::ParticipantUnknown(value) => participant_reference(&value.request),
        ServerValue::NoBinding(value) => binding_required(&value.request),
        ServerValue::StaleAuthority(value) => stale_authority(value),
        ServerValue::Retired(value) => match value {
            crate::wire::Retired::Enrollment { request, .. } => enrollment(request),
            crate::wire::Retired::Participant { request, .. } => participant_reference(request),
        },
        ServerValue::MarkerClosureCapacityExceeded(value) => closure_envelope(&value.request),
        ServerValue::EnrollBound(value)
        | ServerValue::Bound(ReceiptReplay::Enrollment(value))
        | ServerValue::UnboundReceipt(ReceiptReplay::Enrollment(value)) => enroll_bound(value),
        ServerValue::EnrollmentKnown(value) => {
            RequestKey::Enrollment(value.conversation_id, value.token)
        }
        ServerValue::ReceiptExpired(value) => receipt_expired(value),
        ServerValue::ReceiptCapacityExceeded(value) => match value {
            crate::wire::ReceiptCapacityExceeded::Enrollment { request, .. } => enrollment(request),
            crate::wire::ReceiptCapacityExceeded::CredentialAttach { request, .. } => {
                attach(request)
            }
        },
        ServerValue::IdentityCapacityExceeded(value) => enrollment(&value.request),
        ServerValue::ObserverBackpressure(value) => observer_backpressure(value),
        ServerValue::ConversationSequenceExhausted(value) => sequence_envelope(&value.request),
        ServerValue::AttachBound(value)
        | ServerValue::Bound(ReceiptReplay::CredentialAttach(value))
        | ServerValue::UnboundReceipt(ReceiptReplay::CredentialAttach(value)) => {
            attach_bound(value)
        }
        ServerValue::StaleOrUnknownReceipt(value) => RequestKey::Attach(
            value.conversation_id,
            value.participant_id,
            value.presented_generation,
            value.token,
            value.presented_marker_delivery_seq,
        ),
        ServerValue::MarkerNotDelivered(value) => marker_proof(&value.request),
        ServerValue::MarkerMismatch(value) => marker_proof(&value.request),
        ServerValue::DetachCommitted(value) => RequestKey::Detach(
            value.conversation_id(),
            value.participant_id(),
            value.capability_generation(),
            value.detach_attempt_token(),
        ),
        ServerValue::DetachInProgress(value) => RequestKey::Detach(
            value.conversation_id,
            value.participant_id,
            value.presented_generation,
            value.presented_token,
        ),
        ServerValue::AckCommitted(value) => participant_ack(value.request()),
        ServerValue::AckNoOp(value) => match value {
            AckNoOp::ParticipantAck(request) => participant_ack(request),
            AckNoOp::MarkerAck(request) => marker_ack(request),
        },
        ServerValue::AckGap(value) => participant_ack(value.request()),
        ServerValue::AckRegression(value) => participant_ack(value.request()),
        ServerValue::LeaveCommitted(value) => RequestKey::Leave(
            value.conversation_id(),
            value.participant_id(),
            value.presented_generation(),
            value.leave_attempt_token(),
        ),
        ServerValue::MarkerAckCommitted(value) => marker_ack(value.request()),
        ServerValue::RecordCommitted(value) => record(value.request()),
        ServerValue::RecordTooLarge(value) => record(&value.request),
        ServerValue::ObserverRecoveryAccepted(_)
        | ServerValue::InvalidObserverEpoch(_)
        | ServerValue::InvalidObserverEpochList(_) => RequestKey::ObserverRecovery(0),
    };
    match key {
        RequestKey::Record(conversation, participant, generation, _) => RequestKey::Record(
            conversation,
            participant,
            generation,
            ambiguous_authorization,
        ),
        RequestKey::ObserverRecovery(_) => RequestKey::ObserverRecovery(ambiguous_authorization),
        other => other,
    }
}

const fn attempt_conflict(value: &crate::wire::AttemptTokenBodyConflict) -> RequestKey {
    match value {
        crate::wire::AttemptTokenBodyConflict::CredentialAttach {
            token,
            conversation_id,
            presented_participant_id,
            presented_generation,
            presented_marker_delivery_seq,
            ..
        } => RequestKey::Attach(
            *conversation_id,
            *presented_participant_id,
            *presented_generation,
            *token,
            *presented_marker_delivery_seq,
        ),
        crate::wire::AttemptTokenBodyConflict::Leave {
            token,
            conversation_id,
            presented_participant_id,
            presented_generation,
        } => RequestKey::Leave(
            *conversation_id,
            *presented_participant_id,
            *presented_generation,
            *token,
        ),
    }
}

const fn connection_capacity(
    value: &crate::wire::ConnectionConversationCapacityExceeded,
) -> RequestKey {
    match value {
        crate::wire::ConnectionConversationCapacityExceeded::SemanticRequest {
            request, ..
        } => response_envelope(request),
        crate::wire::ConnectionConversationCapacityExceeded::ObserverRecovery { .. } => {
            RequestKey::ObserverRecovery(0)
        }
    }
}

const fn binding_occupied(
    value: &crate::wire::ConnectionConversationBindingOccupied,
) -> RequestKey {
    match value {
        crate::wire::ConnectionConversationBindingOccupied::Enrollment {
            conversation_id,
            enrollment_token,
        } => RequestKey::Enrollment(*conversation_id, *enrollment_token),
        crate::wire::ConnectionConversationBindingOccupied::CredentialAttach {
            conversation_id,
            participant_id,
            capability_generation,
            attach_attempt_token,
            accept_marker_delivery_seq,
        } => RequestKey::Attach(
            *conversation_id,
            *participant_id,
            *capability_generation,
            *attach_attempt_token,
            *accept_marker_delivery_seq,
        ),
    }
}

const fn enrollment(value: &EnrollmentEnvelope) -> RequestKey {
    RequestKey::Enrollment(value.conversation_id, value.enrollment_token)
}

const fn attach(value: &AttachEnvelope) -> RequestKey {
    RequestKey::Attach(
        value.conversation_id,
        value.participant_id,
        value.capability_generation,
        value.attach_attempt_token,
        value.accept_marker_delivery_seq,
    )
}

const fn detach(value: &DetachEnvelope) -> RequestKey {
    RequestKey::Detach(
        value.conversation_id,
        value.participant_id,
        value.capability_generation,
        value.detach_attempt_token,
    )
}

const fn participant_ack(value: &ParticipantAckEnvelope) -> RequestKey {
    RequestKey::Ack(
        value.conversation_id,
        value.participant_id,
        value.capability_generation,
        value.through_seq,
    )
}

const fn leave(value: &LeaveEnvelope) -> RequestKey {
    RequestKey::Leave(
        value.conversation_id,
        value.participant_id,
        value.capability_generation,
        value.leave_attempt_token,
    )
}

const fn marker_ack(value: &MarkerAckEnvelope) -> RequestKey {
    RequestKey::MarkerAck(
        value.conversation_id,
        value.participant_id,
        value.capability_generation,
        value.marker_delivery_seq,
    )
}

const fn record(value: &RecordAdmissionEnvelope) -> RequestKey {
    RequestKey::Record(
        value.conversation_id,
        value.participant_id,
        value.capability_generation,
        0,
    )
}

const fn response_envelope(value: &ResponseEnvelope) -> RequestKey {
    match value {
        ResponseEnvelope::Enrollment(value) => enrollment(value),
        ResponseEnvelope::CredentialAttach(value) => attach(value),
        ResponseEnvelope::Detach(value) => detach(value),
        ResponseEnvelope::ParticipantAck(value) => participant_ack(value),
        ResponseEnvelope::Leave(value) => leave(value),
        ResponseEnvelope::MarkerAck(value) => marker_ack(value),
        ResponseEnvelope::RecordAdmission(value) => record(value),
    }
}

const fn participant_reference(value: &ParticipantReferenceEnvelope) -> RequestKey {
    match value {
        ParticipantReferenceEnvelope::CredentialAttach(value) => attach(value),
        ParticipantReferenceEnvelope::Detach(value) => detach(value),
        ParticipantReferenceEnvelope::ParticipantAck(value) => participant_ack(value),
        ParticipantReferenceEnvelope::Leave(value) => leave(value),
        ParticipantReferenceEnvelope::MarkerAck(value) => marker_ack(value),
        ParticipantReferenceEnvelope::RecordAdmission(value) => record(value),
    }
}

const fn binding_required(value: &BindingRequiredEnvelope) -> RequestKey {
    match value {
        BindingRequiredEnvelope::Detach(value) => detach(value),
        BindingRequiredEnvelope::ParticipantAck(value) => participant_ack(value),
        BindingRequiredEnvelope::Leave(value) => leave(value),
        BindingRequiredEnvelope::MarkerAck(value) => marker_ack(value),
        BindingRequiredEnvelope::RecordAdmission(value) => record(value),
    }
}

const fn stale_authority(value: &StaleAuthority) -> RequestKey {
    match value {
        StaleAuthority::Live { request, .. } => match request {
            CommonStaleAuthorityEnvelope::CredentialAttach(value) => attach(value),
            CommonStaleAuthorityEnvelope::ParticipantAck(value) => participant_ack(value),
            CommonStaleAuthorityEnvelope::MarkerAck(value) => marker_ack(value),
            CommonStaleAuthorityEnvelope::RecordAdmission(value) => record(value),
        },
        StaleAuthority::Detach(value) => match value {
            DetachStaleAuthority::Live {
                conversation_id,
                participant_id,
                capability_generation,
                detach_attempt_token,
                ..
            } => RequestKey::Detach(
                *conversation_id,
                *participant_id,
                *capability_generation,
                *detach_attempt_token,
            ),
            DetachStaleAuthority::TerminalizedDetachCell(value) => RequestKey::Detach(
                value.conversation_id(),
                value.participant_id(),
                value.capability_generation(),
                value.detach_attempt_token(),
            ),
        },
        StaleAuthority::Leave(value) => match value {
            LeaveStaleAuthority::Live {
                conversation_id,
                participant_id,
                presented_generation,
                leave_attempt_token,
                ..
            }
            | LeaveStaleAuthority::CommittedLeaveTombstone {
                conversation_id,
                participant_id,
                presented_generation,
                leave_attempt_token,
                ..
            } => RequestKey::Leave(
                *conversation_id,
                *participant_id,
                *presented_generation,
                *leave_attempt_token,
            ),
        },
    }
}

const fn closure_envelope(value: &ClosureCheckedEnvelope) -> RequestKey {
    match value {
        ClosureCheckedEnvelope::Enrollment(value) => enrollment(value),
        ClosureCheckedEnvelope::CredentialAttach(value) => attach(value),
        ClosureCheckedEnvelope::Leave(value) => leave(value),
        ClosureCheckedEnvelope::RecordAdmission(value) => record(value),
    }
}

const fn order_envelope(value: &OrderAllocatingEnvelope) -> RequestKey {
    match value {
        OrderAllocatingEnvelope::Enrollment(value) => enrollment(value),
        OrderAllocatingEnvelope::CredentialAttach(value) => attach(value),
        OrderAllocatingEnvelope::RecordAdmission(value) => record(value),
    }
}

const fn sequence_envelope(value: &SequenceAllocatingEnvelope) -> RequestKey {
    match value {
        SequenceAllocatingEnvelope::Enrollment(value) => enrollment(value),
        SequenceAllocatingEnvelope::CredentialAttach(value) => attach(value),
        SequenceAllocatingEnvelope::RecordAdmission(value) => record(value),
    }
}

const fn enroll_bound(value: &crate::wire::EnrollBound) -> RequestKey {
    RequestKey::Enrollment(value.conversation_id(), value.token())
}

const fn attach_bound(value: &crate::wire::AttachBound) -> RequestKey {
    RequestKey::Attach(
        value.conversation_id(),
        value.participant_id(),
        value.request_generation(),
        value.token(),
        value.accepted_marker_delivery_seq(),
    )
}

const fn receipt_expired(value: &ReceiptExpired) -> RequestKey {
    match value {
        ReceiptExpired::Enrollment {
            conversation_id,
            token,
            ..
        } => RequestKey::Enrollment(*conversation_id, *token),
        ReceiptExpired::CredentialAttach {
            conversation_id,
            token,
            participant_id,
            presented_generation,
            presented_marker_delivery_seq,
            ..
        } => RequestKey::Attach(
            *conversation_id,
            *participant_id,
            *presented_generation,
            *token,
            *presented_marker_delivery_seq,
        ),
    }
}

const fn observer_backpressure(value: &ObserverBackpressure) -> RequestKey {
    match value {
        ObserverBackpressure::Enrollment { request, .. } => enrollment(request),
        ObserverBackpressure::CredentialAttach { request, .. } => attach(request),
        ObserverBackpressure::Detach { request, .. } => detach(request),
        ObserverBackpressure::Leave { request, .. } => leave(request),
        ObserverBackpressure::RecordAdmission { request, .. } => record(request),
    }
}

const fn marker_proof(value: &MarkerProofRequest) -> RequestKey {
    match value {
        MarkerProofRequest::CredentialAttach(value) => RequestKey::Attach(
            value.conversation_id,
            value.participant_id,
            value.capability_generation,
            value.token,
            Some(value.requested_marker_delivery_seq),
        ),
        MarkerProofRequest::MarkerAck(value) => RequestKey::MarkerAck(
            value.conversation_id,
            value.participant_id,
            value.capability_generation,
            value.requested_marker_delivery_seq,
        ),
    }
}
