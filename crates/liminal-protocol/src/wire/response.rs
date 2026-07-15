use alloc::{boxed::Box, string::String, vec::Vec};

use crate::algebra::{ResourceDimension, ResourceVector};

use super::{
    AckGapReason, AckRegressionReason, AttachAttemptToken, AttachEnvelope, AttachSecret,
    AttemptConflict, BindingEpoch, ClientDiscriminant, ClosureCheckedEnvelope, ConversationId,
    Counter, DecodeClass, DeliverySeq, DetachAttemptToken, DetachEnvelope, EnrollmentEnvelope,
    EnrollmentToken, Generation, IdentityCapacityScope, InvalidObserverEpochListReason,
    InvalidObserverEpochReason, LeaveAttemptToken, LeaveEnvelope, MarkerAckEnvelope,
    MarkerClosureCapacityExceeded, MarkerMismatchReason, MarkerNotDeliveredReason, ObserverEpoch,
    ParticipantAckEnvelope, ParticipantId, ProtocolVersion, ReceiptCapacityScope,
    ReceiptExpiryReason, RecordAdmissionEnvelope, ResponseEnvelope, SequenceBudget,
    ServerDiscriminant,
};

pub use super::tags::{DetachAuthorityStateTag, LeaveAuthorityStateTag, ResourceDimensionTag};

/// Exact pre-semantic participant transport rejection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParticipantTransportRejected {
    /// Selected exact reason body.
    pub reason: TransportRejectionReason,
}

/// Closed transport-rejection reason union.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransportRejectionReason {
    /// Declared complete frame exceeds the current allocation bound.
    FrameTooLarge {
        /// Header plus declared payload bytes.
        complete_frame_bytes: u64,
        /// Active negotiated or pre-capability frame limit.
        max_frame_bytes: u64,
    },
    /// Structural participant decoding failed.
    DecodeFailed {
        /// Exact structural failure class.
        decode_class: DecodeClass,
    },
    /// Concrete participant version is unsupported.
    UnsupportedVersion {
        /// Version found in the inner prefix.
        presented_version: ProtocolVersion,
        /// Stored or server-supported expected version.
        supported_version: ProtocolVersion,
    },
    /// Connection authentication failed.
    AuthenticationFailed,
    /// The connection did not negotiate participant capability.
    ///
    /// The serialized `required_capability` value is always exactly
    /// `"participant-v1"`.
    ParticipantCapabilityRequired,
}

/// Credential-attach or Leave token was reused with a different canonical body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AttemptTokenBodyConflict {
    /// Credential attach conflict; operation tag is fixed to attach.
    CredentialAttach {
        /// Presented attach token.
        token: AttachAttemptToken,
        /// Conversation from the conflicting request.
        conversation_id: ConversationId,
        /// Presented participant.
        presented_participant_id: ParticipantId,
        /// Presented generation.
        presented_generation: Generation,
        /// Presented marker option.
        presented_marker_delivery_seq: Option<DeliverySeq>,
        /// Generation or marker conflict, tested in that order.
        conflict: AttemptConflict,
    },
    /// Leave conflict; only generation conflict is constructible.
    Leave {
        /// Presented Leave token.
        token: LeaveAttemptToken,
        /// Conversation from the conflicting request.
        conversation_id: ConversationId,
        /// Presented participant.
        presented_participant_id: ParticipantId,
        /// Presented generation.
        presented_generation: Generation,
    },
}

/// Generic semantic connection-conversation capacity refusal (`0x0102`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectionConversationCapacityExceeded {
    /// Exact triggering request envelope.
    pub request: ResponseEnvelope,
    /// Negotiated connection-conversation limit.
    pub limit: u64,
}

/// Exact binding-slot occupancy response; occupying identity is never disclosed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConnectionConversationBindingOccupied {
    /// Enrollment; the encoded presented-participant option is exactly `None`.
    Enrollment {
        /// Conversation from the request.
        conversation_id: ConversationId,
        /// Enrollment token from the request.
        enrollment_token: EnrollmentToken,
    },
    /// Credential attach; the encoded option is `Some(participant_id)`.
    CredentialAttach {
        /// Conversation from the request.
        conversation_id: ConversationId,
        /// Presented participant.
        participant_id: ParticipantId,
        /// Presented generation.
        capability_generation: Generation,
        /// Attach token from the request.
        attach_attempt_token: AttachAttemptToken,
        /// Presented marker option.
        accept_marker_delivery_seq: Option<DeliverySeq>,
    },
}

/// Request kinds that may require an unreserved transaction-order major.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OrderAllocatingEnvelope {
    /// Enrollment.
    Enrollment(EnrollmentEnvelope),
    /// Credential attach.
    CredentialAttach(AttachEnvelope),
    /// Ordinary record admission.
    RecordAdmission(RecordAdmissionEnvelope),
}

/// Exhausted conversation transaction order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConversationOrderExhausted {
    /// Exact triggering request envelope.
    pub request: OrderAllocatingEnvelope,
    /// Counter is always [`Counter::TransactionOrder`].
    pub counter: Counter,
    /// Current high allocated major.
    pub high: u64,
    /// Checked next major, absent after maximum allocation.
    pub next_value: Option<u64>,
    /// Current unreserved majors remaining.
    pub order_remaining: u128,
    /// Current `A + X + RO + RA` claims.
    pub reserved_claims: u128,
    /// Simulated remaining majors.
    pub resulting_order_remaining: u128,
    /// Simulated four-term reserved claims.
    pub resulting_reserved_claims: u128,
}

impl ConversationOrderExhausted {
    /// Exact required-major count serialized by protocol v1.
    pub const REQUIRED_MAJORS: u64 = 1;
}

/// Participant-naming envelopes eligible for unknown/retired classification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParticipantReferenceEnvelope {
    /// Credential attach.
    CredentialAttach(AttachEnvelope),
    /// Detach.
    Detach(DetachEnvelope),
    /// Continuous acknowledgement.
    ParticipantAck(ParticipantAckEnvelope),
    /// Leave.
    Leave(LeaveEnvelope),
    /// Marker acknowledgement.
    MarkerAck(MarkerAckEnvelope),
    /// Ordinary record admission.
    RecordAdmission(RecordAdmissionEnvelope),
}

/// Binding-required request envelopes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BindingRequiredEnvelope {
    /// Detach.
    Detach(DetachEnvelope),
    /// Continuous acknowledgement.
    ParticipantAck(ParticipantAckEnvelope),
    /// Bound Leave.
    Leave(LeaveEnvelope),
    /// Marker acknowledgement.
    MarkerAck(MarkerAckEnvelope),
    /// Ordinary record admission.
    RecordAdmission(RecordAdmissionEnvelope),
}

/// Unknown participant outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParticipantUnknown {
    /// Exact triggering request envelope.
    pub request: ParticipantReferenceEnvelope,
}

/// Missing required binding outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NoBinding {
    /// Exact triggering request envelope.
    pub request: BindingRequiredEnvelope,
}

/// Current binding view carried only by terminalized-detach authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BindingStateView {
    /// A current binding exists.
    Bound {
        /// Current binding epoch.
        current_binding_epoch: BindingEpoch,
    },
    /// No current binding exists.
    Detached,
}

impl BindingStateView {
    /// Returns the exact nested binding-state tag.
    #[must_use]
    pub const fn tag(self) -> super::BindingStateTag {
        match self {
            Self::Bound { .. } => super::BindingStateTag::Bound,
            Self::Detached => super::BindingStateTag::Detached,
        }
    }
}

/// Data retained by the mandated terminalized detach cell.
///
/// Fields are private. External callers can obtain this response only through
/// the lifecycle module's verified terminalized-cell transition or wire decode.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TerminalizedDetachCell {
    conversation_id: ConversationId,
    participant_id: ParticipantId,
    capability_generation: Generation,
    detach_attempt_token: DetachAttemptToken,
    current_generation: Generation,
    committed_binding_epoch: BindingEpoch,
    binding_state: BindingStateView,
}

impl TerminalizedDetachCell {
    /// Constructs the server-side semantic response from the mandated fourth
    /// detach-cell variant.
    ///
    /// Taking [`crate::lifecycle::TerminalizedDetach`] here is intentional: the
    /// three-variant detach model rejected by
    /// `docs/design/LP-EXTRACTION-GOAL.md` cannot call this constructor because
    /// it has no state carrying the old committed binding epoch.
    pub(crate) const fn from_terminalized_state<V>(
        state: &crate::lifecycle::TerminalizedDetach<V>,
        conversation_id: ConversationId,
        current_generation: Generation,
        binding_state: BindingStateView,
    ) -> Self {
        Self {
            conversation_id,
            participant_id: state.participant_id(),
            capability_generation: state.request_generation(),
            detach_attempt_token: state.token(),
            current_generation,
            committed_binding_epoch: state.committed_binding_epoch(),
            binding_state,
        }
    }

    /// Conversation from the old detach request.
    #[must_use]
    pub const fn conversation_id(&self) -> ConversationId {
        self.conversation_id
    }

    /// Participant from the old detach request.
    #[must_use]
    pub const fn participant_id(&self) -> ParticipantId {
        self.participant_id
    }

    /// Presented generation from the old detach request.
    #[must_use]
    pub const fn capability_generation(&self) -> Generation {
        self.capability_generation
    }

    /// Old detach attempt token.
    #[must_use]
    pub const fn detach_attempt_token(&self) -> DetachAttemptToken {
        self.detach_attempt_token
    }

    /// Current live generation.
    #[must_use]
    pub const fn current_generation(&self) -> Generation {
        self.current_generation
    }

    /// Old committed binding epoch retained by terminalization.
    #[must_use]
    pub const fn committed_binding_epoch(&self) -> BindingEpoch {
        self.committed_binding_epoch
    }

    /// Current bound/detached view.
    #[must_use]
    pub const fn binding_state(&self) -> BindingStateView {
        self.binding_state
    }
}

/// Detach-specific stale-authority tagged union.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DetachStaleAuthority {
    /// Ordinary live generation mismatch.
    Live {
        /// Conversation from the request.
        conversation_id: ConversationId,
        /// Participant from the request.
        participant_id: ParticipantId,
        /// Presented generation.
        capability_generation: Generation,
        /// Presented detach token.
        detach_attempt_token: DetachAttemptToken,
        /// Current generation.
        current_generation: Generation,
    },
    /// Verified exact old token resolved to a terminalized detach cell.
    TerminalizedDetachCell(TerminalizedDetachCell),
}

impl DetachStaleAuthority {
    /// Returns the detach-specific outer authority-state tag.
    #[must_use]
    pub const fn authority_state_tag(&self) -> DetachAuthorityStateTag {
        match self {
            Self::Live { .. } => DetachAuthorityStateTag::Live,
            Self::TerminalizedDetachCell(_) => DetachAuthorityStateTag::TerminalizedDetachCell,
        }
    }
}

/// Leave-specific stale-authority tagged union.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeaveStaleAuthority {
    /// Live generation or secret mismatch.
    Live {
        /// Conversation from the request.
        conversation_id: ConversationId,
        /// Participant from the request.
        participant_id: ParticipantId,
        /// Presented generation.
        presented_generation: Generation,
        /// Presented Leave token.
        leave_attempt_token: LeaveAttemptToken,
        /// Current generation.
        current_generation: Generation,
    },
    /// Exact committed Leave token with a mismatching secret.
    CommittedLeaveTombstone {
        /// Conversation from the request.
        conversation_id: ConversationId,
        /// Participant from the request.
        participant_id: ParticipantId,
        /// Presented generation.
        presented_generation: Generation,
        /// Presented Leave token.
        leave_attempt_token: LeaveAttemptToken,
        /// Permanent retired generation.
        retired_generation: Generation,
    },
}

impl LeaveStaleAuthority {
    /// Returns the Leave-specific outer authority-state tag.
    #[must_use]
    pub const fn authority_state_tag(&self) -> LeaveAuthorityStateTag {
        match self {
            Self::Live { .. } => LeaveAuthorityStateTag::Live,
            Self::CommittedLeaveTombstone { .. } => LeaveAuthorityStateTag::CommittedLeaveTombstone,
        }
    }
}

/// Common-envelope live stale-authority alternatives.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommonStaleAuthorityEnvelope {
    /// Credential attach.
    CredentialAttach(AttachEnvelope),
    /// Continuous acknowledgement.
    ParticipantAck(ParticipantAckEnvelope),
    /// Marker acknowledgement.
    MarkerAck(MarkerAckEnvelope),
    /// Ordinary record admission.
    RecordAdmission(RecordAdmissionEnvelope),
}

/// Complete stale-authority outcome payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StaleAuthority {
    /// Generic live authority mismatch.
    Live {
        /// Exact triggering request envelope.
        request: CommonStaleAuthorityEnvelope,
        /// Current live generation.
        current_generation: Generation,
    },
    /// Detach-specific complete replacement schema.
    Detach(DetachStaleAuthority),
    /// Leave-specific complete replacement schema.
    Leave(LeaveStaleAuthority),
}

/// Tombstone classification, including enrollment's additional participant id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Retired {
    /// Enrollment mapping resolved to a tombstone.
    Enrollment {
        /// Original request envelope.
        request: EnrollmentEnvelope,
        /// Mapped permanent participant.
        participant_id: ParticipantId,
        /// Permanent retired generation.
        retired_generation: Generation,
    },
    /// Participant-naming request resolved to its tombstone.
    Participant {
        /// Exact triggering request envelope.
        request: ParticipantReferenceEnvelope,
        /// Permanent retired generation.
        retired_generation: Generation,
    },
}

/// Canonical successful enrollment receipt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnrollBound {
    conversation_id: ConversationId,
    token: EnrollmentToken,
    participant_id: ParticipantId,
    attach_secret: AttachSecret,
    origin_binding_epoch: BindingEpoch,
    receipt_expires_at: u128,
    provenance_expires_at: u128,
}

impl EnrollBound {
    /// Creates an enrollment result only for the protocol's fixed generation 1.
    ///
    /// Returns `None` when the origin binding epoch does not carry generation 1.
    /// The required wire fields `request_generation`, `persisted_cursor`, and
    /// `accepted_marker_delivery_seq` are synthesized as `None`, zero, and
    /// `None`, respectively.
    #[must_use]
    pub const fn new(
        conversation_id: ConversationId,
        token: EnrollmentToken,
        participant_id: ParticipantId,
        attach_secret: AttachSecret,
        origin_binding_epoch: BindingEpoch,
        receipt_expires_at: u128,
        provenance_expires_at: u128,
    ) -> Option<Self> {
        if origin_binding_epoch.capability_generation.get() == 1 {
            Some(Self {
                conversation_id,
                token,
                participant_id,
                attach_secret,
                origin_binding_epoch,
                receipt_expires_at,
                provenance_expires_at,
            })
        } else {
            None
        }
    }

    /// Conversation from the request.
    #[must_use]
    pub const fn conversation_id(&self) -> ConversationId {
        self.conversation_id
    }

    /// Enrollment token echoed as the result token.
    #[must_use]
    pub const fn token(&self) -> EnrollmentToken {
        self.token
    }

    /// Minted participant.
    #[must_use]
    pub const fn participant_id(&self) -> ParticipantId {
        self.participant_id
    }

    /// Required absent request generation.
    #[must_use]
    pub const fn request_generation(&self) -> Option<Generation> {
        None
    }

    /// Fixed enrollment result generation 1.
    #[must_use]
    pub const fn capability_generation(&self) -> Generation {
        self.origin_binding_epoch.capability_generation
    }

    /// Newly minted attach secret.
    #[must_use]
    pub const fn attach_secret(&self) -> AttachSecret {
        self.attach_secret
    }

    /// Origin binding epoch.
    #[must_use]
    pub const fn origin_binding_epoch(&self) -> BindingEpoch {
        self.origin_binding_epoch
    }

    /// Fixed persisted cursor zero.
    #[must_use]
    pub const fn persisted_cursor(&self) -> DeliverySeq {
        0
    }

    /// Required absent accepted-marker field.
    #[must_use]
    pub const fn accepted_marker_delivery_seq(&self) -> Option<DeliverySeq> {
        None
    }

    /// Receipt deadline.
    #[must_use]
    pub const fn receipt_expires_at(&self) -> u128 {
        self.receipt_expires_at
    }

    /// Provenance deadline.
    #[must_use]
    pub const fn provenance_expires_at(&self) -> u128 {
        self.provenance_expires_at
    }
}

/// Enrollment token maps to a known live participant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnrollmentKnown {
    /// Conversation from the request.
    pub conversation_id: ConversationId,
    /// Enrollment token.
    pub token: EnrollmentToken,
    /// Mapped participant.
    pub participant_id: ParticipantId,
    /// Current live generation.
    pub current_generation: Generation,
}

/// Exact expired/superseded receipt response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReceiptExpired {
    /// Enrollment provenance; marker field is structurally absent.
    Enrollment {
        /// Conversation from the request.
        conversation_id: ConversationId,
        /// Enrollment token.
        token: EnrollmentToken,
        /// Mapped participant.
        participant_id: ParticipantId,
        /// Result generation retained by provenance.
        result_generation: Generation,
        /// Current live generation.
        current_generation: Generation,
        /// Deadline or supersession.
        reason: ReceiptExpiryReason,
    },
    /// Credential-attach provenance.
    CredentialAttach {
        /// Conversation from the request.
        conversation_id: ConversationId,
        /// Attach token.
        token: AttachAttemptToken,
        /// Participant from the request.
        participant_id: ParticipantId,
        /// Originally presented generation.
        presented_generation: Generation,
        /// Originally presented marker option.
        presented_marker_delivery_seq: Option<DeliverySeq>,
        /// Result generation retained by provenance.
        result_generation: Generation,
        /// Current live generation.
        current_generation: Generation,
        /// Deadline or supersession.
        reason: ReceiptExpiryReason,
    },
}

/// Receipt/provenance scopes reachable from enrollment.
///
/// Per-participant occupancy is zero before a new identity exists, so the two
/// per-participant refusal arms are deliberately unconstructible here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EnrollmentReceiptCapacityScope {
    /// Server-wide live receipt rows.
    LiveReceiptServer,
    /// Server-wide provenance rows.
    ProvenanceServer,
    /// Per-conversation provenance rows.
    ProvenanceConversation,
}

impl EnrollmentReceiptCapacityScope {
    /// Returns the shared five-value wire registry entry.
    #[must_use]
    pub const fn wire_scope(self) -> ReceiptCapacityScope {
        match self {
            Self::LiveReceiptServer => ReceiptCapacityScope::LiveReceiptServer,
            Self::ProvenanceServer => ReceiptCapacityScope::ProvenanceServer,
            Self::ProvenanceConversation => ReceiptCapacityScope::ProvenanceConversation,
        }
    }
}

/// Receipt/provenance capacity refusal with origin-specific valid scopes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReceiptCapacityExceeded {
    /// Enrollment capacity refusal.
    Enrollment {
        /// Enrollment request envelope.
        request: EnrollmentEnvelope,
        /// One of the three scopes reachable before identity mint.
        scope: EnrollmentReceiptCapacityScope,
        /// Signed scope limit.
        limit: u64,
        /// Current occupancy.
        occupied: u64,
    },
    /// Credential-attach capacity refusal.
    CredentialAttach {
        /// Credential-attach request envelope.
        request: AttachEnvelope,
        /// First full scope in the exact five-scope order.
        scope: ReceiptCapacityScope,
        /// Signed scope limit.
        limit: u64,
        /// Current occupancy.
        occupied: u64,
    },
}

impl ReceiptCapacityExceeded {
    /// Exact requested-row count serialized by protocol v1.
    pub const REQUESTED: u64 = 1;
}

/// Enrollment identity-capacity refusal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdentityCapacityExceeded {
    /// Enrollment request envelope.
    pub request: EnrollmentEnvelope,
    /// Server or conversation scope.
    pub scope: IdentityCapacityScope,
    /// Signed scope limit.
    pub limit: u64,
    /// Current occupancy.
    pub occupied: u64,
}

impl IdentityCapacityExceeded {
    /// Exact requested-identity count serialized by protocol v1.
    pub const REQUESTED: u64 = 1;
}

/// Common observer-backpressure suffix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObserverBackpressureState {
    /// Refusal epoch.
    pub backpressure_epoch: ObserverEpoch,
    /// Observer progress captured by the refusal.
    pub observer_progress: DeliverySeq,
}

/// Exact operation-specific observer-backpressure payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObserverBackpressure {
    /// Enrollment.
    Enrollment {
        /// Request envelope.
        request: EnrollmentEnvelope,
        /// Refusal state.
        state: ObserverBackpressureState,
    },
    /// Credential attach.
    CredentialAttach {
        /// Request envelope.
        request: AttachEnvelope,
        /// Refusal state.
        state: ObserverBackpressureState,
    },
    /// Detach, which additionally exposes the committed old binding epoch.
    Detach {
        /// Request envelope.
        request: DetachEnvelope,
        /// Binding epoch the detach is terminalizing.
        committed_binding_epoch: BindingEpoch,
        /// Refusal state.
        state: ObserverBackpressureState,
    },
    /// Leave, which additionally reports whether an older terminal cell exists.
    Leave {
        /// Request envelope.
        request: LeaveEnvelope,
        /// Refusal state.
        state: ObserverBackpressureState,
        /// Whether an earlier terminal cell exists.
        prior_terminal_cell_exists: bool,
    },
    /// Ordinary admission.
    RecordAdmission {
        /// Request envelope.
        request: RecordAdmissionEnvelope,
        /// Refusal state.
        state: ObserverBackpressureState,
    },
}

/// Request alternatives that can exhaust optional sequence admission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SequenceAllocatingEnvelope {
    /// Enrollment.
    Enrollment(EnrollmentEnvelope),
    /// Credential attach.
    CredentialAttach(AttachEnvelope),
    /// Ordinary record admission.
    RecordAdmission(RecordAdmissionEnvelope),
}

/// Canonical sequence-exhaustion response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConversationSequenceExhausted {
    /// Exact triggering request envelope.
    pub request: SequenceAllocatingEnvelope,
    /// Exactly one canonical ten-field budget.
    pub sequence_budget: SequenceBudget,
}

/// Canonical successful credential-attach receipt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttachBound {
    /// Conversation from the request.
    pub conversation_id: ConversationId,
    /// Attach token echoed as the result token.
    pub token: AttachAttemptToken,
    /// Participant from the request.
    pub participant_id: ParticipantId,
    /// Originally presented generation.
    pub request_generation: Generation,
    /// Newly minted result generation.
    pub capability_generation: Generation,
    /// Newly minted attach secret.
    pub attach_secret: AttachSecret,
    /// Origin binding epoch.
    pub origin_binding_epoch: BindingEpoch,
    /// Persisted participant cursor.
    pub persisted_cursor: DeliverySeq,
    /// Marker accepted atomically by recovery.
    pub accepted_marker_delivery_seq: Option<DeliverySeq>,
    /// Receipt deadline.
    pub receipt_expires_at: u128,
    /// Provenance deadline.
    pub provenance_expires_at: u128,
}

/// Attach receipt is no longer known after provenance expiry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StaleOrUnknownReceipt {
    /// Conversation from the request.
    pub conversation_id: ConversationId,
    /// Attach token.
    pub token: AttachAttemptToken,
    /// Participant from the request.
    pub participant_id: ParticipantId,
    /// Originally presented generation.
    pub presented_generation: Generation,
    /// Originally presented marker option.
    pub presented_marker_delivery_seq: Option<DeliverySeq>,
    /// Current live generation.
    pub current_generation: Generation,
}

/// Attach marker-proof request fields; attach token is part of the replacement schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttachMarkerProof {
    /// Conversation from the request.
    pub conversation_id: ConversationId,
    /// Attach token from the request.
    pub token: AttachAttemptToken,
    /// Participant from the request.
    pub participant_id: ParticipantId,
    /// Presented generation.
    pub capability_generation: Generation,
    /// Explicit requested marker.
    pub requested_marker_delivery_seq: DeliverySeq,
}

/// Marker-ack proof request fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarkerAckProof {
    /// Conversation from the request.
    pub conversation_id: ConversationId,
    /// Participant from the request.
    pub participant_id: ParticipantId,
    /// Presented generation.
    pub capability_generation: Generation,
    /// Explicit requested marker.
    pub requested_marker_delivery_seq: DeliverySeq,
}

/// Marker-proof request alternatives.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MarkerProofRequest {
    /// Credential attach proof.
    CredentialAttach(AttachMarkerProof),
    /// Marker acknowledgement proof.
    MarkerAck(MarkerAckProof),
}

/// Requested marker was not delivered to the proof epoch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarkerNotDelivered {
    /// Complete flattened request fields.
    pub request: MarkerProofRequest,
    /// Singleton reason tag.
    pub reason: MarkerNotDeliveredReason,
    /// Marker actually expected by current state.
    pub expected_marker_delivery_seq: DeliverySeq,
}

/// Exact marker-mismatch reason body; no optional field bag exists.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarkerMismatchBody {
    /// Requested marker is below current cursor.
    BelowCursor {
        /// Current participant cursor.
        current_cursor: DeliverySeq,
    },
    /// No marker is expected by current state.
    NoMarkerExpected,
    /// A different marker is expected.
    ExpectedDifferentMarker {
        /// Expected marker sequence.
        expected_marker_delivery_seq: DeliverySeq,
    },
}

impl MarkerMismatchBody {
    /// Returns the stable reason selector.
    #[must_use]
    pub const fn reason(self) -> MarkerMismatchReason {
        match self {
            Self::BelowCursor { .. } => MarkerMismatchReason::BelowCursor,
            Self::NoMarkerExpected => MarkerMismatchReason::NoMarkerExpected,
            Self::ExpectedDifferentMarker { .. } => MarkerMismatchReason::ExpectedDifferentMarker,
        }
    }
}

/// Presented marker does not match current marker state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarkerMismatch {
    /// Complete flattened request fields.
    pub request: MarkerProofRequest,
    /// Selected exact reason body.
    pub mismatch: MarkerMismatchBody,
}

/// Complete canonical receipt replay payload used by Bound/UnboundReceipt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReceiptReplay {
    /// Enrollment receipt replay; request generation is encoded `None`.
    Enrollment {
        /// Conversation from the request.
        conversation_id: ConversationId,
        /// Enrollment token.
        token: EnrollmentToken,
        /// Participant result.
        participant_id: ParticipantId,
        /// Result generation.
        capability_generation: Generation,
        /// Stored attach secret.
        attach_secret: AttachSecret,
        /// Origin binding epoch.
        origin_binding_epoch: BindingEpoch,
        /// Persisted cursor.
        persisted_cursor: DeliverySeq,
        /// Receipt deadline.
        receipt_expires_at: u128,
        /// Provenance deadline.
        provenance_expires_at: u128,
    },
    /// Credential-attach receipt replay.
    CredentialAttach {
        /// Conversation from the request.
        conversation_id: ConversationId,
        /// Attach token.
        token: AttachAttemptToken,
        /// Participant result.
        participant_id: ParticipantId,
        /// Originally presented generation.
        request_generation: Generation,
        /// Result generation.
        capability_generation: Generation,
        /// Stored attach secret.
        attach_secret: AttachSecret,
        /// Origin binding epoch.
        origin_binding_epoch: BindingEpoch,
        /// Persisted cursor.
        persisted_cursor: DeliverySeq,
        /// Marker accepted by the original attach.
        accepted_marker_delivery_seq: Option<DeliverySeq>,
        /// Receipt deadline.
        receipt_expires_at: u128,
        /// Provenance deadline.
        provenance_expires_at: u128,
    },
}

/// Stable committed detach response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DetachCommitted {
    /// Conversation from the request.
    pub conversation_id: ConversationId,
    /// Participant from the request.
    pub participant_id: ParticipantId,
    /// Presented generation.
    pub capability_generation: Generation,
    /// Committed detach token.
    pub detach_attempt_token: DetachAttemptToken,
    /// Binding epoch ended by detach.
    pub committed_binding_epoch: BindingEpoch,
    /// Assigned Detached delivery sequence.
    pub detached_delivery_seq: DeliverySeq,
}

/// Different detach token encountered an existing pending cell.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DetachInProgress {
    /// Conversation from the competing request.
    pub conversation_id: ConversationId,
    /// Participant from the competing request.
    pub participant_id: ParticipantId,
    /// Competing presented token; stored token is never disclosed.
    pub presented_token: DetachAttemptToken,
    /// Competing presented generation.
    pub presented_generation: Generation,
    /// Binding epoch being terminalized by the pending cell.
    pub committed_binding_epoch: BindingEpoch,
}

/// Continuous acknowledgement advanced the cursor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AckCommitted {
    /// Request envelope.
    pub request: ParticipantAckEnvelope,
    /// Resulting committed cursor.
    pub current_cursor: DeliverySeq,
}

/// Idempotent normal or marker acknowledgement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AckNoOp {
    /// Continuous acknowledgement.
    ParticipantAck {
        /// Request envelope.
        request: ParticipantAckEnvelope,
        /// Unchanged cursor.
        current_cursor: DeliverySeq,
    },
    /// Marker acknowledgement.
    MarkerAck {
        /// Request envelope.
        request: MarkerAckEnvelope,
        /// Unchanged cursor.
        current_cursor: DeliverySeq,
    },
}

/// Continuous acknowledgement crossed a gap.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AckGap {
    /// Request envelope.
    pub request: ParticipantAckEnvelope,
    /// Unchanged cursor.
    pub current_cursor: DeliverySeq,
    /// Singleton gap reason.
    pub reason: AckGapReason,
}

/// Continuous acknowledgement regressed below the cursor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AckRegression {
    /// Request envelope.
    pub request: ParticipantAckEnvelope,
    /// Unchanged cursor.
    pub current_cursor: DeliverySeq,
    /// Singleton regression reason.
    pub reason: AckRegressionReason,
}

/// Permanent terminal Leave result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaveCommitted {
    /// Conversation from the request.
    pub conversation_id: ConversationId,
    /// Committed Leave token.
    pub leave_attempt_token: LeaveAttemptToken,
    /// Retired participant.
    pub participant_id: ParticipantId,
    /// Presented generation.
    pub presented_generation: Generation,
    /// Permanent retired generation.
    pub retired_generation: Generation,
    /// Active binding ended by this same commit, if any.
    pub ended_binding_epoch: Option<BindingEpoch>,
    /// Earlier binding-terminal record, if one exists.
    pub prior_terminal_delivery_seq: Option<DeliverySeq>,
    /// Assigned Left delivery sequence.
    pub left_delivery_seq: DeliverySeq,
}

/// Marker acknowledgement advanced the cursor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarkerAckCommitted {
    /// Marker-ack request envelope.
    pub request: MarkerAckEnvelope,
    /// Resulting marker cursor.
    pub current_cursor: DeliverySeq,
}

/// Ordinary record commit result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordCommitted {
    /// Request envelope, without opaque payload.
    pub request: RecordAdmissionEnvelope,
    /// Verified sender; equal to the request participant.
    pub sender_participant_id: ParticipantId,
    /// Assigned record sequence.
    pub delivery_seq: DeliverySeq,
}

/// Ordinary record exceeds configured entry or byte maximum.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordTooLarge {
    /// Request envelope, without opaque payload.
    pub request: RecordAdmissionEnvelope,
    /// First failing component.
    pub dimension: ResourceDimension,
    /// Exact durable record charge.
    pub encoded_record_charge: ResourceVector,
    /// Configured maximum ordinary record charge.
    pub max_ordinary_record_charge: ResourceVector,
}

/// One observer progress status returned in request order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObserverProgressStatus {
    /// Conversation from the request entry.
    pub conversation_id: ConversationId,
    /// Presented refusal epoch.
    pub refused_epoch: ObserverEpoch,
    /// Current observer progress.
    pub current_observer_progress: DeliverySeq,
    /// Whether an equal epoch was atomically armed.
    pub armed: bool,
    /// Whether the presented epoch was already older/progressed.
    pub progressed: bool,
}

/// Whole-batch observer recovery success.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObserverRecoveryAccepted {
    /// Request-ordered statuses. Wire uses one structural `u64` count only.
    pub statuses: Vec<ObserverProgressStatus>,
}

/// Whole-batch invalid observer epoch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InvalidObserverEpoch {
    /// Conversation does not exist; current progress option is encoded `None`.
    ConversationUnknown {
        /// Unknown conversation.
        conversation_id: ConversationId,
        /// Presented epoch.
        presented_epoch: ObserverEpoch,
    },
    /// Presented epoch is ahead; current progress option is encoded `Some`.
    EpochAhead {
        /// Known conversation.
        conversation_id: ConversationId,
        /// Presented newer epoch.
        presented_epoch: ObserverEpoch,
        /// Current observer progress.
        current_observer_progress: DeliverySeq,
    },
}

impl InvalidObserverEpoch {
    /// Returns the exact scalar reason tag implied by the selected body.
    #[must_use]
    pub const fn reason(&self) -> InvalidObserverEpochReason {
        match self {
            Self::ConversationUnknown { .. } => InvalidObserverEpochReason::ConversationUnknown,
            Self::EpochAhead { .. } => InvalidObserverEpochReason::EpochAhead,
        }
    }
}

/// Whole-batch invalid observer recovery list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InvalidObserverEpochList {
    /// Request exceeds its signed entry limit.
    TooManyEntries {
        /// Presented list length.
        presented_entries: u64,
        /// Signed maximum entries.
        max_entries: u64,
    },
    /// Request repeats a conversation.
    DuplicateConversation {
        /// Repeated conversation.
        conversation_id: ConversationId,
        /// First request index.
        first_index: u64,
        /// Repeated request index.
        duplicate_index: u64,
    },
}

impl InvalidObserverEpochList {
    /// Returns the exact scalar reason tag implied by the selected body.
    #[must_use]
    pub const fn reason(&self) -> InvalidObserverEpochListReason {
        match self {
            Self::TooManyEntries { .. } => InvalidObserverEpochListReason::TooManyEntries,
            Self::DuplicateConversation { .. } => {
                InvalidObserverEpochListReason::DuplicateConversation
            }
        }
    }
}

/// Recovery-batch connection-capacity refusal (`0x0124`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObserverRecoveryConnectionCapacityExceeded {
    /// First request-ordered conversation that would exceed the limit.
    pub conversation_id: ConversationId,
    /// Signed connection-conversation limit.
    pub limit: u64,
}

/// Exhaustive server-to-client semantic participant value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServerValue {
    /// `0x0100`.
    ParticipantTransportRejected(ParticipantTransportRejected),
    /// `0x0101`.
    AttemptTokenBodyConflict(AttemptTokenBodyConflict),
    /// `0x0102`.
    ConnectionConversationCapacityExceeded(ConnectionConversationCapacityExceeded),
    /// `0x0103`.
    ConnectionConversationBindingOccupied(ConnectionConversationBindingOccupied),
    /// `0x0104`.
    ConversationOrderExhausted(Box<ConversationOrderExhausted>),
    /// `0x0105`.
    ParticipantUnknown(ParticipantUnknown),
    /// `0x0106`.
    NoBinding(NoBinding),
    /// `0x0107`.
    StaleAuthority(StaleAuthority),
    /// `0x0108`.
    Retired(Retired),
    /// `0x0109`.
    MarkerClosureCapacityExceeded(Box<MarkerClosureCapacityExceeded>),
    /// `0x010A`.
    EnrollBound(EnrollBound),
    /// `0x010B`.
    EnrollmentKnown(EnrollmentKnown),
    /// `0x010C`.
    ReceiptExpired(ReceiptExpired),
    /// `0x010D`.
    ReceiptCapacityExceeded(ReceiptCapacityExceeded),
    /// `0x010E`.
    IdentityCapacityExceeded(IdentityCapacityExceeded),
    /// `0x010F`.
    ObserverBackpressure(ObserverBackpressure),
    /// `0x0110`.
    ConversationSequenceExhausted(Box<ConversationSequenceExhausted>),
    /// `0x0111`.
    AttachBound(AttachBound),
    /// `0x0112`.
    StaleOrUnknownReceipt(StaleOrUnknownReceipt),
    /// `0x0113`.
    MarkerNotDelivered(MarkerNotDelivered),
    /// `0x0114`.
    MarkerMismatch(MarkerMismatch),
    /// `0x0115`.
    Bound(ReceiptReplay),
    /// `0x0116`.
    UnboundReceipt(ReceiptReplay),
    /// `0x0117`.
    DetachCommitted(DetachCommitted),
    /// `0x0118`.
    DetachInProgress(DetachInProgress),
    /// `0x0119`.
    AckCommitted(AckCommitted),
    /// `0x011A`.
    AckNoOp(AckNoOp),
    /// `0x011B`.
    AckGap(AckGap),
    /// `0x011C`.
    AckRegression(AckRegression),
    /// `0x011D`.
    LeaveCommitted(LeaveCommitted),
    /// `0x011E`.
    MarkerAckCommitted(MarkerAckCommitted),
    /// `0x011F`.
    RecordCommitted(RecordCommitted),
    /// `0x0120`.
    RecordTooLarge(RecordTooLarge),
    /// `0x0121`.
    ObserverRecoveryAccepted(ObserverRecoveryAccepted),
    /// `0x0122`.
    InvalidObserverEpoch(InvalidObserverEpoch),
    /// `0x0123`.
    InvalidObserverEpochList(InvalidObserverEpochList),
    /// `0x0124`; semantic meaning is connection-conversation capacity exceeded.
    ObserverRecoveryConnectionCapacityExceeded(ObserverRecoveryConnectionCapacityExceeded),
}

impl ServerValue {
    /// Returns the exact contiguous server value discriminant.
    #[must_use]
    pub const fn discriminant(&self) -> ServerDiscriminant {
        match self {
            Self::ParticipantTransportRejected(_) => {
                ServerDiscriminant::ParticipantTransportRejected
            }
            Self::AttemptTokenBodyConflict(_) => ServerDiscriminant::AttemptTokenBodyConflict,
            Self::ConnectionConversationCapacityExceeded(_) => {
                ServerDiscriminant::ConnectionConversationCapacityExceeded
            }
            Self::ConnectionConversationBindingOccupied(_) => {
                ServerDiscriminant::ConnectionConversationBindingOccupied
            }
            Self::ConversationOrderExhausted(_) => ServerDiscriminant::ConversationOrderExhausted,
            Self::ParticipantUnknown(_) => ServerDiscriminant::ParticipantUnknown,
            Self::NoBinding(_) => ServerDiscriminant::NoBinding,
            Self::StaleAuthority(_) => ServerDiscriminant::StaleAuthority,
            Self::Retired(_) => ServerDiscriminant::Retired,
            Self::MarkerClosureCapacityExceeded(_) => {
                ServerDiscriminant::MarkerClosureCapacityExceeded
            }
            Self::EnrollBound(_) => ServerDiscriminant::EnrollBound,
            Self::EnrollmentKnown(_) => ServerDiscriminant::EnrollmentKnown,
            Self::ReceiptExpired(_) => ServerDiscriminant::ReceiptExpired,
            Self::ReceiptCapacityExceeded(_) => ServerDiscriminant::ReceiptCapacityExceeded,
            Self::IdentityCapacityExceeded(_) => ServerDiscriminant::IdentityCapacityExceeded,
            Self::ObserverBackpressure(_) => ServerDiscriminant::ObserverBackpressure,
            Self::ConversationSequenceExhausted(_) => {
                ServerDiscriminant::ConversationSequenceExhausted
            }
            Self::AttachBound(_) => ServerDiscriminant::AttachBound,
            Self::StaleOrUnknownReceipt(_) => ServerDiscriminant::StaleOrUnknownReceipt,
            Self::MarkerNotDelivered(_) => ServerDiscriminant::MarkerNotDelivered,
            Self::MarkerMismatch(_) => ServerDiscriminant::MarkerMismatch,
            Self::Bound(_) => ServerDiscriminant::Bound,
            Self::UnboundReceipt(_) => ServerDiscriminant::UnboundReceipt,
            Self::DetachCommitted(_) => ServerDiscriminant::DetachCommitted,
            Self::DetachInProgress(_) => ServerDiscriminant::DetachInProgress,
            Self::AckCommitted(_) => ServerDiscriminant::AckCommitted,
            Self::AckNoOp(_) => ServerDiscriminant::AckNoOp,
            Self::AckGap(_) => ServerDiscriminant::AckGap,
            Self::AckRegression(_) => ServerDiscriminant::AckRegression,
            Self::LeaveCommitted(_) => ServerDiscriminant::LeaveCommitted,
            Self::MarkerAckCommitted(_) => ServerDiscriminant::MarkerAckCommitted,
            Self::RecordCommitted(_) => ServerDiscriminant::RecordCommitted,
            Self::RecordTooLarge(_) => ServerDiscriminant::RecordTooLarge,
            Self::ObserverRecoveryAccepted(_) => ServerDiscriminant::ObserverRecoveryAccepted,
            Self::InvalidObserverEpoch(_) => ServerDiscriminant::InvalidObserverEpoch,
            Self::InvalidObserverEpochList(_) => ServerDiscriminant::InvalidObserverEpochList,
            Self::ObserverRecoveryConnectionCapacityExceeded(_) => {
                ServerDiscriminant::ObserverRecoveryConnectionCapacityExceeded
            }
        }
    }

    /// Returns the structural originating-request selector when the value has one.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub const fn originating_request(&self) -> Option<ClientDiscriminant> {
        match self {
            Self::ParticipantTransportRejected(_)
            | Self::ObserverRecoveryAccepted(_)
            | Self::InvalidObserverEpoch(_)
            | Self::InvalidObserverEpochList(_)
            | Self::ObserverRecoveryConnectionCapacityExceeded(_) => None,
            Self::AttemptTokenBodyConflict(value) => Some(match value {
                AttemptTokenBodyConflict::CredentialAttach { .. } => {
                    ClientDiscriminant::CredentialAttachRequest
                }
                AttemptTokenBodyConflict::Leave { .. } => ClientDiscriminant::LeaveRequest,
            }),
            Self::ConnectionConversationCapacityExceeded(value) => {
                Some(value.request.originating_request())
            }
            Self::ConnectionConversationBindingOccupied(value) => Some(match value {
                ConnectionConversationBindingOccupied::Enrollment { .. } => {
                    ClientDiscriminant::EnrollmentRequest
                }
                ConnectionConversationBindingOccupied::CredentialAttach { .. } => {
                    ClientDiscriminant::CredentialAttachRequest
                }
            }),
            Self::ConversationOrderExhausted(value) => Some(match &value.request {
                OrderAllocatingEnvelope::Enrollment(_) => ClientDiscriminant::EnrollmentRequest,
                OrderAllocatingEnvelope::CredentialAttach(_) => {
                    ClientDiscriminant::CredentialAttachRequest
                }
                OrderAllocatingEnvelope::RecordAdmission(_) => ClientDiscriminant::RecordAdmission,
            }),
            Self::ParticipantUnknown(value) => Some(participant_reference_origin(&value.request)),
            Self::NoBinding(value) => Some(binding_required_origin(&value.request)),
            Self::StaleAuthority(value) => Some(stale_authority_origin(value)),
            Self::Retired(value) => Some(match value {
                Retired::Enrollment { .. } => ClientDiscriminant::EnrollmentRequest,
                Retired::Participant { request, .. } => participant_reference_origin(request),
            }),
            Self::MarkerClosureCapacityExceeded(value) => Some(match &value.request {
                ClosureCheckedEnvelope::Enrollment(_) => ClientDiscriminant::EnrollmentRequest,
                ClosureCheckedEnvelope::CredentialAttach(_) => {
                    ClientDiscriminant::CredentialAttachRequest
                }
                ClosureCheckedEnvelope::Leave(_) => ClientDiscriminant::LeaveRequest,
                ClosureCheckedEnvelope::RecordAdmission(_) => ClientDiscriminant::RecordAdmission,
            }),
            Self::EnrollBound(_) | Self::EnrollmentKnown(_) | Self::IdentityCapacityExceeded(_) => {
                Some(ClientDiscriminant::EnrollmentRequest)
            }
            Self::ReceiptExpired(value) => Some(match value {
                ReceiptExpired::Enrollment { .. } => ClientDiscriminant::EnrollmentRequest,
                ReceiptExpired::CredentialAttach { .. } => {
                    ClientDiscriminant::CredentialAttachRequest
                }
            }),
            Self::ReceiptCapacityExceeded(value) => Some(match value {
                ReceiptCapacityExceeded::Enrollment { .. } => ClientDiscriminant::EnrollmentRequest,
                ReceiptCapacityExceeded::CredentialAttach { .. } => {
                    ClientDiscriminant::CredentialAttachRequest
                }
            }),
            Self::ObserverBackpressure(value) => Some(match value {
                ObserverBackpressure::Enrollment { .. } => ClientDiscriminant::EnrollmentRequest,
                ObserverBackpressure::CredentialAttach { .. } => {
                    ClientDiscriminant::CredentialAttachRequest
                }
                ObserverBackpressure::Detach { .. } => ClientDiscriminant::DetachRequest,
                ObserverBackpressure::Leave { .. } => ClientDiscriminant::LeaveRequest,
                ObserverBackpressure::RecordAdmission { .. } => ClientDiscriminant::RecordAdmission,
            }),
            Self::ConversationSequenceExhausted(value) => Some(match &value.request {
                SequenceAllocatingEnvelope::Enrollment(_) => ClientDiscriminant::EnrollmentRequest,
                SequenceAllocatingEnvelope::CredentialAttach(_) => {
                    ClientDiscriminant::CredentialAttachRequest
                }
                SequenceAllocatingEnvelope::RecordAdmission(_) => {
                    ClientDiscriminant::RecordAdmission
                }
            }),
            Self::AttachBound(_) | Self::StaleOrUnknownReceipt(_) => {
                Some(ClientDiscriminant::CredentialAttachRequest)
            }
            Self::MarkerNotDelivered(value) => Some(marker_proof_origin(&value.request)),
            Self::MarkerMismatch(value) => Some(marker_proof_origin(&value.request)),
            Self::Bound(value) | Self::UnboundReceipt(value) => Some(match value {
                ReceiptReplay::Enrollment { .. } => ClientDiscriminant::EnrollmentRequest,
                ReceiptReplay::CredentialAttach { .. } => {
                    ClientDiscriminant::CredentialAttachRequest
                }
            }),
            Self::DetachCommitted(_) | Self::DetachInProgress(_) => {
                Some(ClientDiscriminant::DetachRequest)
            }
            Self::AckCommitted(_) | Self::AckGap(_) | Self::AckRegression(_) => {
                Some(ClientDiscriminant::ParticipantAck)
            }
            Self::AckNoOp(value) => Some(match value {
                AckNoOp::ParticipantAck { .. } => ClientDiscriminant::ParticipantAck,
                AckNoOp::MarkerAck { .. } => ClientDiscriminant::MarkerAck,
            }),
            Self::LeaveCommitted(_) => Some(ClientDiscriminant::LeaveRequest),
            Self::MarkerAckCommitted(_) => Some(ClientDiscriminant::MarkerAck),
            Self::RecordCommitted(_) | Self::RecordTooLarge(_) => {
                Some(ClientDiscriminant::RecordAdmission)
            }
        }
    }
}

const fn participant_reference_origin(
    request: &ParticipantReferenceEnvelope,
) -> ClientDiscriminant {
    match request {
        ParticipantReferenceEnvelope::CredentialAttach(_) => {
            ClientDiscriminant::CredentialAttachRequest
        }
        ParticipantReferenceEnvelope::Detach(_) => ClientDiscriminant::DetachRequest,
        ParticipantReferenceEnvelope::ParticipantAck(_) => ClientDiscriminant::ParticipantAck,
        ParticipantReferenceEnvelope::Leave(_) => ClientDiscriminant::LeaveRequest,
        ParticipantReferenceEnvelope::MarkerAck(_) => ClientDiscriminant::MarkerAck,
        ParticipantReferenceEnvelope::RecordAdmission(_) => ClientDiscriminant::RecordAdmission,
    }
}

const fn binding_required_origin(request: &BindingRequiredEnvelope) -> ClientDiscriminant {
    match request {
        BindingRequiredEnvelope::Detach(_) => ClientDiscriminant::DetachRequest,
        BindingRequiredEnvelope::ParticipantAck(_) => ClientDiscriminant::ParticipantAck,
        BindingRequiredEnvelope::Leave(_) => ClientDiscriminant::LeaveRequest,
        BindingRequiredEnvelope::MarkerAck(_) => ClientDiscriminant::MarkerAck,
        BindingRequiredEnvelope::RecordAdmission(_) => ClientDiscriminant::RecordAdmission,
    }
}

const fn stale_authority_origin(value: &StaleAuthority) -> ClientDiscriminant {
    match value {
        StaleAuthority::Live { request, .. } => match request {
            CommonStaleAuthorityEnvelope::CredentialAttach(_) => {
                ClientDiscriminant::CredentialAttachRequest
            }
            CommonStaleAuthorityEnvelope::ParticipantAck(_) => ClientDiscriminant::ParticipantAck,
            CommonStaleAuthorityEnvelope::MarkerAck(_) => ClientDiscriminant::MarkerAck,
            CommonStaleAuthorityEnvelope::RecordAdmission(_) => ClientDiscriminant::RecordAdmission,
        },
        StaleAuthority::Detach(_) => ClientDiscriminant::DetachRequest,
        StaleAuthority::Leave(_) => ClientDiscriminant::LeaveRequest,
    }
}

const fn marker_proof_origin(request: &MarkerProofRequest) -> ClientDiscriminant {
    match request {
        MarkerProofRequest::CredentialAttach(_) => ClientDiscriminant::CredentialAttachRequest,
        MarkerProofRequest::MarkerAck(_) => ClientDiscriminant::MarkerAck,
    }
}

/// Exact capability string serialized by transport capability refusal.
pub const PARTICIPANT_CAPABILITY: &str = "participant-v1";

/// Returns the exact attempt operation implied by a body-conflict variant.
#[must_use]
pub const fn attempt_operation(value: &AttemptTokenBodyConflict) -> super::AttemptOperation {
    match value {
        AttemptTokenBodyConflict::CredentialAttach { .. } => {
            super::AttemptOperation::CredentialAttachRequest
        }
        AttemptTokenBodyConflict::Leave { .. } => super::AttemptOperation::LeaveRequest,
    }
}

/// Owns a capability string after wire decoding while retaining domain validation.
#[must_use]
pub fn capability_string() -> String {
    String::from(PARTICIPANT_CAPABILITY)
}
