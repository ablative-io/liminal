use super::{
    AttachAttemptToken, ClientDiscriminant, ConversationId, DeliverySeq, DetachAttemptToken,
    EnrollmentToken, Generation, LeaveAttemptToken, ParticipantId, RecordAdmissionAttemptToken,
};

/// Enrollment response common envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnrollmentEnvelope {
    /// Conversation from the request.
    pub conversation_id: ConversationId,
    /// Enrollment token from the request.
    pub enrollment_token: EnrollmentToken,
}

/// Credential-attach response common envelope; the secret is deliberately absent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttachEnvelope {
    /// Conversation from the request.
    pub conversation_id: ConversationId,
    /// Participant from the request.
    pub participant_id: ParticipantId,
    /// Presented generation.
    pub capability_generation: Generation,
    /// Attach token from the request.
    pub attach_attempt_token: AttachAttemptToken,
    /// Presented marker option.
    pub accept_marker_delivery_seq: Option<DeliverySeq>,
}

/// Detach response common envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DetachEnvelope {
    /// Conversation from the request.
    pub conversation_id: ConversationId,
    /// Participant from the request.
    pub participant_id: ParticipantId,
    /// Presented generation.
    pub capability_generation: Generation,
    /// Detach token from the request.
    pub detach_attempt_token: DetachAttemptToken,
}

/// Continuous-ack response common envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParticipantAckEnvelope {
    /// Conversation from the request.
    pub conversation_id: ConversationId,
    /// Participant from the request.
    pub participant_id: ParticipantId,
    /// Presented generation.
    pub capability_generation: Generation,
    /// Requested cumulative boundary.
    pub through_seq: DeliverySeq,
}

/// Leave response common envelope; the secret is deliberately absent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaveEnvelope {
    /// Conversation from the request.
    pub conversation_id: ConversationId,
    /// Participant from the request.
    pub participant_id: ParticipantId,
    /// Presented generation.
    pub capability_generation: Generation,
    /// Leave token from the request.
    pub leave_attempt_token: LeaveAttemptToken,
}

/// Marker-ack response common envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarkerAckEnvelope {
    /// Conversation from the request.
    pub conversation_id: ConversationId,
    /// Participant from the request.
    pub participant_id: ParticipantId,
    /// Presented generation.
    pub capability_generation: Generation,
    /// Requested marker sequence.
    pub marker_delivery_seq: DeliverySeq,
}

/// Ordinary-admission response common envelope; payload is deliberately absent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordAdmissionEnvelope {
    /// Conversation from the request.
    pub conversation_id: ConversationId,
    /// Participant from the request.
    pub participant_id: ParticipantId,
    /// Presented generation.
    pub capability_generation: Generation,
    /// Client-selected request-attempt identity echoed by every terminal response.
    pub record_admission_attempt_token: RecordAdmissionAttemptToken,
}

/// Exact operation-specific response envelope for requests `0x0001..=0x0007`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResponseEnvelope {
    /// Enrollment envelope.
    Enrollment(EnrollmentEnvelope),
    /// Credential-attach envelope.
    CredentialAttach(AttachEnvelope),
    /// Detach envelope.
    Detach(DetachEnvelope),
    /// Continuous-ack envelope.
    ParticipantAck(ParticipantAckEnvelope),
    /// Leave envelope.
    Leave(LeaveEnvelope),
    /// Marker-ack envelope.
    MarkerAck(MarkerAckEnvelope),
    /// Ordinary-admission envelope.
    RecordAdmission(RecordAdmissionEnvelope),
}

impl ResponseEnvelope {
    /// Returns the structural `originating_request` selector.
    #[must_use]
    pub const fn originating_request(&self) -> ClientDiscriminant {
        match self {
            Self::Enrollment(_) => ClientDiscriminant::EnrollmentRequest,
            Self::CredentialAttach(_) => ClientDiscriminant::CredentialAttachRequest,
            Self::Detach(_) => ClientDiscriminant::DetachRequest,
            Self::ParticipantAck(_) => ClientDiscriminant::ParticipantAck,
            Self::Leave(_) => ClientDiscriminant::LeaveRequest,
            Self::MarkerAck(_) => ClientDiscriminant::MarkerAck,
            Self::RecordAdmission(_) => ClientDiscriminant::RecordAdmission,
        }
    }
}
