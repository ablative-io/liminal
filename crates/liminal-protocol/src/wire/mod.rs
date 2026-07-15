//! Participant message discriminants, schemas, and deterministic wire codec.

mod closure;
mod codec;
mod envelope;
mod primitives;
mod push;
mod request;
mod response;
mod sequence_budget;
mod tags;

#[cfg(test)]
mod codec_tests;
#[cfg(test)]
mod tags_tests;

pub use closure::{
    ClosureCapacityReason, ClosureCheckedEnvelope, ClosureRefusalReason, ClosureSnapshot,
    MarkerClosureCapacityExceeded, ParticipantCursorProgressEdge, RepaymentEdge,
};
pub use codec::{
    CodecError, FRAME_MAX, GENERIC_HEADER_LEN, PARTICIPANT_FRAME_OVERHEAD, PARTICIPANT_FRAME_TYPE,
    PARTICIPANT_PREFIX_LEN, PRECAP_PARTICIPANT_FRAME_MAX, ParticipantFrame, ReceiverDirection,
    complete_frame_bytes, decode, encode, encoded_len,
};
pub use envelope::{
    AttachEnvelope, DetachEnvelope, EnrollmentEnvelope, LeaveEnvelope, MarkerAckEnvelope,
    ParticipantAckEnvelope, RecordAdmissionEnvelope, ResponseEnvelope,
};
pub use primitives::{
    AttachAttemptToken, AttachSecret, BindingEpoch, ConnectionIncarnation, ConversationId,
    DeliverySeq, DetachAttemptToken, EnrollmentToken, Generation, LeaveAttemptToken, ObserverEpoch,
    ParticipantId, ParticipantIndex, ProtocolVersion, TransactionOrder,
};
pub use push::{DetachedCause, DiedCause, ParticipantDelivery, ParticipantRecord, ServerPush};
pub use request::{
    ClientRequest, CredentialAttachRequest, DetachRequest, EnrollmentRequest, LeaveRequest,
    MarkerAck, ObserverRecoveryHandshake, ObserverRefusal, ParticipantAck, RecordAdmission,
};
pub use response::*;
pub use sequence_budget::SequenceBudget;
pub use tags::{
    AckGapReason, AckRegressionReason, AttemptConflict, AttemptOperation, AuthorityStateTag,
    BindingStateTag, ClientDiscriminant, CloseCause, CloseCauseTag, ClosureScope, Counter,
    DecodeClass, IdentityCapacityScope, InvalidObserverEpochListReason, InvalidObserverEpochReason,
    MarkerMismatchReason, MarkerNotDeliveredReason, PushDiscriminant, ReceiptCapacityScope,
    ReceiptExpiryReason, RecordKind, RepaymentEdgeTag, ServerDiscriminant, TagError,
    TransportReasonTag,
};
