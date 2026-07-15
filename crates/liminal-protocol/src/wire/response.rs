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

/// Connection-conversation capacity refusal shared by two exact wire routes.
///
/// The semantic-request arm is carried by `0x0102`; the observer-recovery arm
/// is carried by `0x0124`. Keeping both schemas under this one named outcome
/// follows the frozen contract's R-D1 register while the variants prevent the
/// two different bodies from being confused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConnectionConversationCapacityExceeded {
    /// Decoded semantic request with its exact common envelope (`0x0102`).
    SemanticRequest {
        /// Exact triggering request envelope.
        request: ResponseEnvelope,
        /// Negotiated connection-conversation limit.
        limit: u64,
    },
    /// Observer-recovery request-index preflight refusal (`0x0124`).
    ObserverRecovery {
        /// First request-ordered conversation that would exceed the limit.
        conversation_id: ConversationId,
        /// Signed connection-conversation limit.
        limit: u64,
    },
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
    request: OrderAllocatingEnvelope,
    /// Current high allocated major.
    high: u64,
    /// Current unreserved majors remaining.
    order_remaining: u128,
    /// Current `A + X + RO + RA` claims.
    reserved_claims: u128,
    /// Simulated remaining majors.
    resulting_order_remaining: u128,
    /// Simulated four-term reserved claims.
    resulting_reserved_claims: u128,
}

impl ConversationOrderExhausted {
    /// Exact required-major count serialized by protocol v1.
    pub const REQUIRED_MAJORS: u64 = 1;

    /// Constructs the canonical order-exhaustion snapshot.
    ///
    /// The counter and checked next value are derived rather than accepted from
    /// the caller, making `next_value = Some(high + 1)` (or `None` exactly at
    /// `u64::MAX`) structural.
    #[must_use]
    pub const fn new(
        request: OrderAllocatingEnvelope,
        high: u64,
        order_remaining: u128,
        reserved_claims: u128,
        resulting_order_remaining: u128,
        resulting_reserved_claims: u128,
    ) -> Self {
        Self {
            request,
            high,
            order_remaining,
            reserved_claims,
            resulting_order_remaining,
            resulting_reserved_claims,
        }
    }

    /// Exact triggering request envelope.
    #[must_use]
    pub const fn request(&self) -> &OrderAllocatingEnvelope {
        &self.request
    }

    /// Fixed counter selector.
    #[must_use]
    pub const fn counter(&self) -> Counter {
        let _ = self;
        Counter::TransactionOrder
    }

    /// Current high allocated major.
    #[must_use]
    pub const fn high(&self) -> u64 {
        self.high
    }

    /// Checked next major, absent exactly after allocation of `u64::MAX`.
    #[must_use]
    pub const fn next_value(&self) -> Option<u64> {
        self.high.checked_add(1)
    }

    /// Current unreserved majors remaining.
    #[must_use]
    pub const fn order_remaining(&self) -> u128 {
        self.order_remaining
    }

    /// Current `A + X + RO + RA` claims.
    #[must_use]
    pub const fn reserved_claims(&self) -> u128 {
        self.reserved_claims
    }

    /// Simulated remaining majors.
    #[must_use]
    pub const fn resulting_order_remaining(&self) -> u128 {
        self.resulting_order_remaining
    }

    /// Simulated four-term reserved claims.
    #[must_use]
    pub const fn resulting_reserved_claims(&self) -> u128 {
        self.resulting_reserved_claims
    }
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

    /// Reconstructs the same response from an already-selected wire union arm.
    ///
    /// The authority argument is constructible only inside the server-value
    /// decoder. Ordinary semantic code must use [`Self::from_terminalized_state`],
    /// preserving the compile-time guarantee mandated by
    /// `docs/design/LP-EXTRACTION-GOAL.md`.
    #[allow(clippy::too_many_arguments)]
    pub(super) const fn from_wire_decode(
        _authority: super::server_codec::TerminalizedWireDecodeAuthority,
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        capability_generation: Generation,
        detach_attempt_token: DetachAttemptToken,
        current_generation: Generation,
        committed_binding_epoch: BindingEpoch,
        binding_state: BindingStateView,
    ) -> Self {
        Self {
            conversation_id,
            participant_id,
            capability_generation,
            detach_attempt_token,
            current_generation,
            committed_binding_epoch,
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
    backpressure_epoch: ObserverEpoch,
    /// Observer progress captured by the refusal.
    observer_progress: DeliverySeq,
}

impl ObserverBackpressureState {
    /// Constructs an initial refusal at the current observer-progress baseline.
    ///
    /// An initial refusal epoch is exactly the progress value observed by the
    /// serialized operation.
    #[must_use]
    pub const fn initial(observer_progress: DeliverySeq) -> Self {
        Self {
            backpressure_epoch: observer_progress,
            observer_progress,
        }
    }

    /// Reconstructs an exact-token replay refusal at its current baseline.
    ///
    /// Pending replay at greater progress must drain or atomically rewrite the
    /// cell epoch to that progress before responding. It therefore returns
    /// `None` for any inequality.
    #[must_use]
    pub const fn replay(
        backpressure_epoch: ObserverEpoch,
        observer_progress: DeliverySeq,
    ) -> Option<Self> {
        if backpressure_epoch == observer_progress {
            Some(Self {
                backpressure_epoch,
                observer_progress,
            })
        } else {
            None
        }
    }

    /// Refusal epoch serialized in the response.
    #[must_use]
    pub const fn backpressure_epoch(self) -> ObserverEpoch {
        self.backpressure_epoch
    }

    /// Observer progress captured by the refusal.
    #[must_use]
    pub const fn observer_progress(self) -> DeliverySeq {
        self.observer_progress
    }
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
    conversation_id: ConversationId,
    /// Attach token echoed as the result token.
    token: AttachAttemptToken,
    /// Participant from the request.
    participant_id: ParticipantId,
    /// Originally presented generation.
    request_generation: Generation,
    /// Newly minted attach secret.
    attach_secret: AttachSecret,
    /// Origin binding epoch.
    origin_binding_epoch: BindingEpoch,
    /// Persisted participant cursor.
    persisted_cursor: DeliverySeq,
    /// Marker accepted atomically by recovery.
    accepted_marker_delivery_seq: Option<DeliverySeq>,
    /// Receipt deadline.
    receipt_expires_at: u128,
    /// Provenance deadline.
    provenance_expires_at: u128,
}

impl AttachBound {
    /// Constructs an ordinary attach receipt.
    ///
    /// Returns `None` unless the origin epoch carries the exact checked
    /// successor of `request_generation`. Ordinary attach structurally records
    /// no accepted marker and preserves the supplied cursor.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub const fn ordinary(
        conversation_id: ConversationId,
        token: AttachAttemptToken,
        participant_id: ParticipantId,
        request_generation: Generation,
        attach_secret: AttachSecret,
        origin_binding_epoch: BindingEpoch,
        persisted_cursor: DeliverySeq,
        receipt_expires_at: u128,
        provenance_expires_at: u128,
    ) -> Option<Self> {
        if !is_successor_generation(
            request_generation,
            origin_binding_epoch.capability_generation,
        ) {
            return None;
        }
        Some(Self {
            conversation_id,
            token,
            participant_id,
            request_generation,
            attach_secret,
            origin_binding_epoch,
            persisted_cursor,
            accepted_marker_delivery_seq: None,
            receipt_expires_at,
            provenance_expires_at,
        })
    }

    /// Constructs a fenced-recovery attach receipt.
    ///
    /// Returns `None` unless the origin epoch carries the exact checked
    /// successor of `request_generation`. The accepted marker is also the
    /// resulting persisted cursor by construction.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub const fn fenced(
        conversation_id: ConversationId,
        token: AttachAttemptToken,
        participant_id: ParticipantId,
        request_generation: Generation,
        attach_secret: AttachSecret,
        origin_binding_epoch: BindingEpoch,
        accepted_marker_delivery_seq: DeliverySeq,
        receipt_expires_at: u128,
        provenance_expires_at: u128,
    ) -> Option<Self> {
        if !is_successor_generation(
            request_generation,
            origin_binding_epoch.capability_generation,
        ) {
            return None;
        }
        Some(Self {
            conversation_id,
            token,
            participant_id,
            request_generation,
            attach_secret,
            origin_binding_epoch,
            persisted_cursor: accepted_marker_delivery_seq,
            accepted_marker_delivery_seq: Some(accepted_marker_delivery_seq),
            receipt_expires_at,
            provenance_expires_at,
        })
    }

    /// Conversation from the request.
    #[must_use]
    pub const fn conversation_id(&self) -> ConversationId {
        self.conversation_id
    }

    /// Attach token echoed as the result token.
    #[must_use]
    pub const fn token(&self) -> AttachAttemptToken {
        self.token
    }

    /// Participant from the request.
    #[must_use]
    pub const fn participant_id(&self) -> ParticipantId {
        self.participant_id
    }

    /// Originally presented generation.
    #[must_use]
    pub const fn request_generation(&self) -> Generation {
        self.request_generation
    }

    /// Exact successor capability generation.
    #[must_use]
    pub const fn capability_generation(&self) -> Generation {
        self.origin_binding_epoch.capability_generation
    }

    /// Newly minted attach secret.
    #[must_use]
    pub const fn attach_secret(&self) -> AttachSecret {
        self.attach_secret
    }

    /// Origin binding epoch carrying the result generation.
    #[must_use]
    pub const fn origin_binding_epoch(&self) -> BindingEpoch {
        self.origin_binding_epoch
    }

    /// Persisted participant cursor.
    #[must_use]
    pub const fn persisted_cursor(&self) -> DeliverySeq {
        self.persisted_cursor
    }

    /// Marker accepted atomically by recovery, if this was the fenced path.
    #[must_use]
    pub const fn accepted_marker_delivery_seq(&self) -> Option<DeliverySeq> {
        self.accepted_marker_delivery_seq
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

const fn is_successor_generation(previous: Generation, successor: Generation) -> bool {
    match previous.get().checked_add(1) {
        Some(expected) => successor.get() == expected,
        None => false,
    }
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
    /// Enrollment canonical receipt; generation/cursor/marker constants are
    /// enforced by [`EnrollBound`].
    Enrollment(EnrollBound),
    /// Credential-attach canonical receipt; successor generation and
    /// cursor/marker relations are enforced by [`AttachBound`].
    CredentialAttach(AttachBound),
}

/// Stable committed detach response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DetachCommitted {
    /// Conversation from the request.
    conversation_id: ConversationId,
    /// Participant from the request.
    participant_id: ParticipantId,
    /// Committed detach token.
    detach_attempt_token: DetachAttemptToken,
    /// Binding epoch ended by detach.
    committed_binding_epoch: BindingEpoch,
    /// Assigned Detached delivery sequence.
    detached_delivery_seq: DeliverySeq,
}

impl DetachCommitted {
    /// Constructs a detach result and derives its presented generation from
    /// the binding epoch it ended.
    #[must_use]
    pub const fn new(
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        detach_attempt_token: DetachAttemptToken,
        committed_binding_epoch: BindingEpoch,
        detached_delivery_seq: DeliverySeq,
    ) -> Self {
        Self {
            conversation_id,
            participant_id,
            detach_attempt_token,
            committed_binding_epoch,
            detached_delivery_seq,
        }
    }

    /// Conversation from the request.
    #[must_use]
    pub const fn conversation_id(&self) -> ConversationId {
        self.conversation_id
    }

    /// Participant from the request.
    #[must_use]
    pub const fn participant_id(&self) -> ParticipantId {
        self.participant_id
    }

    /// Presented generation, equal to the committed binding epoch generation.
    #[must_use]
    pub const fn capability_generation(&self) -> Generation {
        self.committed_binding_epoch.capability_generation
    }

    /// Committed detach token.
    #[must_use]
    pub const fn detach_attempt_token(&self) -> DetachAttemptToken {
        self.detach_attempt_token
    }

    /// Binding epoch ended by detach.
    #[must_use]
    pub const fn committed_binding_epoch(&self) -> BindingEpoch {
        self.committed_binding_epoch
    }

    /// Assigned Detached delivery sequence.
    #[must_use]
    pub const fn detached_delivery_seq(&self) -> DeliverySeq {
        self.detached_delivery_seq
    }
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
    request: ParticipantAckEnvelope,
}

impl AckCommitted {
    /// Constructs a committed acknowledgement whose cursor is the requested
    /// cumulative boundary.
    #[must_use]
    pub const fn new(request: ParticipantAckEnvelope) -> Self {
        Self { request }
    }

    /// Request envelope.
    #[must_use]
    pub const fn request(&self) -> &ParticipantAckEnvelope {
        &self.request
    }

    /// Resulting committed cursor, equal to `request.through_seq`.
    #[must_use]
    pub const fn current_cursor(&self) -> DeliverySeq {
        self.request.through_seq
    }
}

/// Idempotent normal or marker acknowledgement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AckNoOp {
    /// Continuous acknowledgement.
    ParticipantAck(ParticipantAckEnvelope),
    /// Marker acknowledgement.
    MarkerAck(MarkerAckEnvelope),
}

impl AckNoOp {
    /// Constructs an idempotent continuous acknowledgement at its requested
    /// cursor.
    #[must_use]
    pub const fn participant_ack(request: ParticipantAckEnvelope) -> Self {
        Self::ParticipantAck(request)
    }

    /// Constructs an idempotent marker acknowledgement at its requested
    /// marker cursor.
    #[must_use]
    pub const fn marker_ack(request: MarkerAckEnvelope) -> Self {
        Self::MarkerAck(request)
    }

    /// Unchanged cursor, derived from the selected request envelope.
    #[must_use]
    pub const fn current_cursor(&self) -> DeliverySeq {
        match self {
            Self::ParticipantAck(request) => request.through_seq,
            Self::MarkerAck(request) => request.marker_delivery_seq,
        }
    }
}

/// Continuous acknowledgement crossed a gap.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AckGap {
    /// Request envelope.
    request: ParticipantAckEnvelope,
    /// Unchanged cursor.
    current_cursor: DeliverySeq,
}

impl AckGap {
    /// Constructs a gap refusal for a requested boundary above the unchanged
    /// cursor.
    #[must_use]
    pub const fn new(request: ParticipantAckEnvelope, current_cursor: DeliverySeq) -> Option<Self> {
        if request.through_seq > current_cursor {
            Some(Self {
                request,
                current_cursor,
            })
        } else {
            None
        }
    }

    /// Request envelope.
    #[must_use]
    pub const fn request(&self) -> &ParticipantAckEnvelope {
        &self.request
    }

    /// Unchanged cursor.
    #[must_use]
    pub const fn current_cursor(&self) -> DeliverySeq {
        self.current_cursor
    }

    /// Fixed gap reason.
    #[must_use]
    pub const fn reason(&self) -> AckGapReason {
        let _ = self;
        AckGapReason::NotContiguouslyAvailable
    }
}

/// Continuous acknowledgement regressed below the cursor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AckRegression {
    /// Request envelope.
    request: ParticipantAckEnvelope,
    /// Unchanged cursor.
    current_cursor: DeliverySeq,
}

impl AckRegression {
    /// Constructs a regression refusal for a requested boundary below the
    /// unchanged cursor.
    #[must_use]
    pub const fn new(request: ParticipantAckEnvelope, current_cursor: DeliverySeq) -> Option<Self> {
        if request.through_seq < current_cursor {
            Some(Self {
                request,
                current_cursor,
            })
        } else {
            None
        }
    }

    /// Request envelope.
    #[must_use]
    pub const fn request(&self) -> &ParticipantAckEnvelope {
        &self.request
    }

    /// Unchanged cursor.
    #[must_use]
    pub const fn current_cursor(&self) -> DeliverySeq {
        self.current_cursor
    }

    /// Fixed regression reason.
    #[must_use]
    pub const fn reason(&self) -> AckRegressionReason {
        let _ = self;
        AckRegressionReason::BelowCursor
    }
}

/// Permanent terminal Leave result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaveCommitted {
    /// Conversation from the request.
    conversation_id: ConversationId,
    /// Committed Leave token.
    leave_attempt_token: LeaveAttemptToken,
    /// Retired participant.
    participant_id: ParticipantId,
    /// Permanent retired generation.
    retired_generation: Generation,
    /// Active binding ended by this same commit, if any.
    ended_binding_epoch: Option<BindingEpoch>,
    /// Earlier binding-terminal record, if one exists.
    prior_terminal_delivery_seq: Option<DeliverySeq>,
    /// Assigned Left delivery sequence.
    left_delivery_seq: DeliverySeq,
}

impl LeaveCommitted {
    /// Constructs a terminal Leave outcome from its authoritative durable
    /// values.
    ///
    /// Returns `None` when a supplied active binding carries another
    /// generation or when a prior terminal is not strictly before `Left`.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        conversation_id: ConversationId,
        leave_attempt_token: LeaveAttemptToken,
        participant_id: ParticipantId,
        retired_generation: Generation,
        ended_binding_epoch: Option<BindingEpoch>,
        prior_terminal_delivery_seq: Option<DeliverySeq>,
        left_delivery_seq: DeliverySeq,
    ) -> Option<Self> {
        if let Some(epoch) = ended_binding_epoch
            && epoch.capability_generation.get() != retired_generation.get()
        {
            return None;
        }
        if let Some(prior) = prior_terminal_delivery_seq
            && prior >= left_delivery_seq
        {
            return None;
        }
        Some(Self {
            conversation_id,
            leave_attempt_token,
            participant_id,
            retired_generation,
            ended_binding_epoch,
            prior_terminal_delivery_seq,
            left_delivery_seq,
        })
    }

    /// Conversation from the request.
    #[must_use]
    pub const fn conversation_id(&self) -> ConversationId {
        self.conversation_id
    }

    /// Committed Leave token.
    #[must_use]
    pub const fn leave_attempt_token(&self) -> LeaveAttemptToken {
        self.leave_attempt_token
    }

    /// Retired participant.
    #[must_use]
    pub const fn participant_id(&self) -> ParticipantId {
        self.participant_id
    }

    /// Presented generation, equal to the permanent retired generation.
    #[must_use]
    pub const fn presented_generation(&self) -> Generation {
        self.retired_generation
    }

    /// Permanent retired generation.
    #[must_use]
    pub const fn retired_generation(&self) -> Generation {
        self.retired_generation
    }

    /// Active binding ended by this same commit, if any.
    #[must_use]
    pub const fn ended_binding_epoch(&self) -> Option<BindingEpoch> {
        self.ended_binding_epoch
    }

    /// Earlier binding-terminal record, if one exists.
    #[must_use]
    pub const fn prior_terminal_delivery_seq(&self) -> Option<DeliverySeq> {
        self.prior_terminal_delivery_seq
    }

    /// Assigned Left delivery sequence.
    #[must_use]
    pub const fn left_delivery_seq(&self) -> DeliverySeq {
        self.left_delivery_seq
    }
}

/// Marker acknowledgement advanced the cursor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarkerAckCommitted {
    /// Marker-ack request envelope.
    request: MarkerAckEnvelope,
}

impl MarkerAckCommitted {
    /// Constructs a committed marker acknowledgement whose cursor is the
    /// requested marker.
    #[must_use]
    pub const fn new(request: MarkerAckEnvelope) -> Self {
        Self { request }
    }

    /// Marker-ack request envelope.
    #[must_use]
    pub const fn request(&self) -> &MarkerAckEnvelope {
        &self.request
    }

    /// Resulting marker cursor, equal to `request.marker_delivery_seq`.
    #[must_use]
    pub const fn current_cursor(&self) -> DeliverySeq {
        self.request.marker_delivery_seq
    }
}

/// Ordinary record commit result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordCommitted {
    /// Request envelope, without opaque payload.
    request: RecordAdmissionEnvelope,
    /// Assigned record sequence.
    delivery_seq: DeliverySeq,
}

impl RecordCommitted {
    /// Constructs an ordinary commit and derives its verified sender from the
    /// authoritative request envelope.
    #[must_use]
    pub const fn new(request: RecordAdmissionEnvelope, delivery_seq: DeliverySeq) -> Self {
        Self {
            request,
            delivery_seq,
        }
    }

    /// Request envelope, without opaque payload.
    #[must_use]
    pub const fn request(&self) -> &RecordAdmissionEnvelope {
        &self.request
    }

    /// Verified sender, exactly the request participant.
    #[must_use]
    pub const fn sender_participant_id(&self) -> ParticipantId {
        self.request.participant_id
    }

    /// Assigned record sequence.
    #[must_use]
    pub const fn delivery_seq(&self) -> DeliverySeq {
        self.delivery_seq
    }
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

/// Exhaustive server-to-client semantic participant value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServerValue {
    /// `0x0100`.
    ParticipantTransportRejected(ParticipantTransportRejected),
    /// `0x0101`.
    AttemptTokenBodyConflict(AttemptTokenBodyConflict),
    /// `0x0102` or `0x0124`, selected by the inner exact schema.
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
            Self::ConnectionConversationCapacityExceeded(value) => match value {
                ConnectionConversationCapacityExceeded::SemanticRequest { .. } => {
                    ServerDiscriminant::ConnectionConversationCapacityExceeded
                }
                ConnectionConversationCapacityExceeded::ObserverRecovery { .. } => {
                    ServerDiscriminant::ObserverRecoveryConnectionCapacityExceeded
                }
            },
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
            | Self::InvalidObserverEpochList(_) => None,
            Self::AttemptTokenBodyConflict(value) => Some(match value {
                AttemptTokenBodyConflict::CredentialAttach { .. } => {
                    ClientDiscriminant::CredentialAttachRequest
                }
                AttemptTokenBodyConflict::Leave { .. } => ClientDiscriminant::LeaveRequest,
            }),
            Self::ConnectionConversationCapacityExceeded(value) => match value {
                ConnectionConversationCapacityExceeded::SemanticRequest { request, .. } => {
                    Some(request.originating_request())
                }
                ConnectionConversationCapacityExceeded::ObserverRecovery { .. } => None,
            },
            Self::ConnectionConversationBindingOccupied(value) => Some(match value {
                ConnectionConversationBindingOccupied::Enrollment { .. } => {
                    ClientDiscriminant::EnrollmentRequest
                }
                ConnectionConversationBindingOccupied::CredentialAttach { .. } => {
                    ClientDiscriminant::CredentialAttachRequest
                }
            }),
            Self::ConversationOrderExhausted(value) => Some(match value.request() {
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
                ReceiptReplay::Enrollment(_) => ClientDiscriminant::EnrollmentRequest,
                ReceiptReplay::CredentialAttach(_) => ClientDiscriminant::CredentialAttachRequest,
            }),
            Self::DetachCommitted(_) | Self::DetachInProgress(_) => {
                Some(ClientDiscriminant::DetachRequest)
            }
            Self::AckCommitted(_) | Self::AckGap(_) | Self::AckRegression(_) => {
                Some(ClientDiscriminant::ParticipantAck)
            }
            Self::AckNoOp(value) => Some(match value {
                AckNoOp::ParticipantAck(_) => ClientDiscriminant::ParticipantAck,
                AckNoOp::MarkerAck(_) => ClientDiscriminant::MarkerAck,
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
