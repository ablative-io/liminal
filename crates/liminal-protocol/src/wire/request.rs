use alloc::vec::Vec;

use super::{
    AttachAttemptToken, AttachSecret, ClientDiscriminant, ConversationId, DeliverySeq,
    DetachAttemptToken, EnrollmentToken, Generation, LeaveAttemptToken, ObserverEpoch,
    ParticipantId, RecordAdmissionAttemptToken,
};

/// Enrollment request body (`0x0001`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnrollmentRequest {
    /// Conversation to enroll in.
    pub conversation_id: ConversationId,
    /// Durable single-purpose enrollment token.
    pub enrollment_token: EnrollmentToken,
}

/// Credential-bearing attach request body (`0x0002`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CredentialAttachRequest {
    /// Conversation containing the participant.
    pub conversation_id: ConversationId,
    /// Permanent participant identity.
    pub participant_id: ParticipantId,
    /// Presented nonzero credential generation.
    pub capability_generation: Generation,
    /// Presented attach secret.
    pub attach_secret: AttachSecret,
    /// Durable single-purpose attach token.
    pub attach_attempt_token: AttachAttemptToken,
    /// Marker accepted atomically by a fenced recovery attach.
    pub accept_marker_delivery_seq: Option<DeliverySeq>,
}

/// Explicit detach request body (`0x0003`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DetachRequest {
    /// Conversation containing the participant.
    pub conversation_id: ConversationId,
    /// Permanent participant identity.
    pub participant_id: ParticipantId,
    /// Presented nonzero credential generation.
    pub capability_generation: Generation,
    /// Durable single-purpose detach token.
    pub detach_attempt_token: DetachAttemptToken,
}

/// Continuous cumulative acknowledgement body (`0x0004`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParticipantAck {
    /// Conversation containing the participant.
    pub conversation_id: ConversationId,
    /// Permanent participant identity.
    pub participant_id: ParticipantId,
    /// Presented nonzero credential generation.
    pub capability_generation: Generation,
    /// Greatest continuously available sequence being acknowledged.
    pub through_seq: DeliverySeq,
}

/// Terminal participant Leave body (`0x0005`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaveRequest {
    /// Conversation containing the participant.
    pub conversation_id: ConversationId,
    /// Permanent participant identity.
    pub participant_id: ParticipantId,
    /// Presented nonzero credential generation.
    pub capability_generation: Generation,
    /// Presented attach secret; it is never echoed in a response envelope.
    pub attach_secret: AttachSecret,
    /// Durable single-purpose Leave token.
    pub leave_attempt_token: LeaveAttemptToken,
}

/// Explicit marker acknowledgement body (`0x0006`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarkerAck {
    /// Conversation containing the participant.
    pub conversation_id: ConversationId,
    /// Permanent participant identity.
    pub participant_id: ParticipantId,
    /// Presented nonzero credential generation.
    pub capability_generation: Generation,
    /// Delivered marker being accepted.
    pub marker_delivery_seq: DeliverySeq,
}

/// Ordinary record-admission body (`0x0007`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordAdmission {
    /// Conversation receiving the record.
    pub conversation_id: ConversationId,
    /// Verified sender participant.
    pub participant_id: ParticipantId,
    /// Presented nonzero credential generation.
    pub capability_generation: Generation,
    /// Client-selected identity of this record-admission request attempt.
    ///
    /// # It must be minted once per RECORD, not once per presentation
    ///
    /// This token is the client's half of ordinary-admission idempotence
    /// (contract amendment A2, §0.13). The server deduplicates on the identity
    /// triple (token, payload fingerprint, verified participant), so an
    /// answer-lost re-present is answered with the original commit only if it
    /// arrives carrying the SAME token.
    ///
    /// Mint it once when the record is staged, persist it beside the staged
    /// bytes, and re-present that exact token after a lost answer — the
    /// write-ahead discipline R-C0 already requires for the tokenized families,
    /// applied one layer up.
    ///
    /// Deriving it per presentation from anything that can change between
    /// attempts silently defeats this. That is not hypothetical: in the field
    /// (2026-08-08, conversation 6) a client derived the token per presentation
    /// with the CURRENT capability generation as an input, a recovery attach
    /// rotated the generation 4 -> 6 between the two presentations, and the
    /// same bytes arrived under two different tokens — committing a
    /// byte-identical second copy at a new sequence.
    ///
    /// The server cannot close this from its side, and deliberately does not
    /// try: two intent-distinct sends of the same body also carry distinct
    /// tokens and MUST remain two commits, so any dedup keyed on payload bytes
    /// alone would collapse a legitimate pair. Both halves are pinned in
    /// `tests_record_admission_dedup`.
    ///
    /// Re-enrolling changes the verified participant and therefore the triple,
    /// so it defeats dedup by design.
    pub record_admission_attempt_token: RecordAdmissionAttemptToken,
    /// Opaque application payload; it is never echoed in a response envelope.
    pub payload: Vec<u8>,
}

/// One observer refusal supplied during reconnect recovery.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObserverRefusal {
    /// Refused conversation.
    pub conversation_id: ConversationId,
    /// Refusal epoch the SDK needs to arm or classify.
    pub refused_epoch: ObserverEpoch,
}

/// One-shot observer-recovery batch body (`0x0008`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObserverRecoveryHandshake {
    /// Request-ordered refusal list. Its wire count is the special `u64` count.
    pub observer_refusals: Vec<ObserverRefusal>,
}

/// Exhaustive client-to-server participant request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientRequest {
    /// `0x0001` enrollment request.
    Enrollment(EnrollmentRequest),
    /// `0x0002` credential attach request.
    CredentialAttach(CredentialAttachRequest),
    /// `0x0003` explicit detach request.
    Detach(DetachRequest),
    /// `0x0004` continuous acknowledgement.
    ParticipantAck(ParticipantAck),
    /// `0x0005` terminal Leave request.
    Leave(LeaveRequest),
    /// `0x0006` marker acknowledgement.
    MarkerAck(MarkerAck),
    /// `0x0007` ordinary record admission.
    RecordAdmission(RecordAdmission),
    /// `0x0008` reconnect recovery batch.
    ObserverRecovery(ObserverRecoveryHandshake),
}

impl ClientRequest {
    /// Returns the stable request discriminant.
    #[must_use]
    pub const fn discriminant(&self) -> ClientDiscriminant {
        match self {
            Self::Enrollment(_) => ClientDiscriminant::EnrollmentRequest,
            Self::CredentialAttach(_) => ClientDiscriminant::CredentialAttachRequest,
            Self::Detach(_) => ClientDiscriminant::DetachRequest,
            Self::ParticipantAck(_) => ClientDiscriminant::ParticipantAck,
            Self::Leave(_) => ClientDiscriminant::LeaveRequest,
            Self::MarkerAck(_) => ClientDiscriminant::MarkerAck,
            Self::RecordAdmission(_) => ClientDiscriminant::RecordAdmission,
            Self::ObserverRecovery(_) => ClientDiscriminant::ObserverRecoveryHandshake,
        }
    }
}
