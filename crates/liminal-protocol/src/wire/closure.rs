use crate::algebra::{ResourceDimension, ResourceVector, WideResourceVector};

use super::{
    AttachEnvelope, BindingEpoch, DeliverySeq, EnrollmentEnvelope, LeaveEnvelope, ParticipantId,
    RecordAdmissionEnvelope, RepaymentEdgeTag,
};

/// Participant-cursor-progress edge payload on the wire.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParticipantCursorProgressEdge {
    /// Participant whose cursor is the witness.
    pub participant_id: ParticipantId,
    /// Binding epoch that received the relevant suffix or marker.
    pub binding_epoch: BindingEpoch,
    /// Continuous cursor boundary witness.
    pub through_seq: DeliverySeq,
    /// Exact delivered marker when marker acknowledgement is required.
    pub marker_delivery_seq: Option<DeliverySeq>,
}

/// Clear state or one of the seven stored repayment edges.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RepaymentEdge {
    /// No edge; legal only at zero debt.
    None,
    /// Observer projection through an exact sequence.
    ObserverProjection {
        /// Required observer boundary.
        through_seq: DeliverySeq,
    },
    /// Physical compaction of an exact retained range.
    PhysicalCompaction {
        /// First retained sequence before completion.
        from_floor: DeliverySeq,
        /// Inclusive sequence that must be compacted.
        through_seq: DeliverySeq,
    },
    /// Delivery of an exact marker to an exact binding.
    MarkerDelivery {
        /// Affected participant.
        participant_id: ParticipantId,
        /// Binding epoch that must receive the marker.
        binding_epoch: BindingEpoch,
        /// Marker sequence.
        marker_delivery_seq: DeliverySeq,
    },
    /// Participant cursor or marker progress.
    ParticipantCursorProgress(ParticipantCursorProgressEdge),
    /// Fenced detached credential recovery.
    DetachedCredentialRecovery {
        /// Detached participant.
        participant_id: ParticipantId,
        /// Delivered marker anchoring recovery.
        marker_delivery_seq: DeliverySeq,
        /// Prior dead binding epoch.
        prior_binding_epoch: BindingEpoch,
    },
    /// Leave-only release of an undelivered marker.
    DetachedMarkerRelease {
        /// Detached participant.
        participant_id: ParticipantId,
        /// Undelivered marker anchor.
        marker_delivery_seq: DeliverySeq,
        /// Last dead binding epoch.
        last_dead_binding_epoch: BindingEpoch,
    },
    /// Leave-only release of a detached cursor witness.
    DetachedCursorRelease {
        /// Detached participant.
        participant_id: ParticipantId,
        /// Last dead binding epoch.
        last_dead_binding_epoch: BindingEpoch,
    },
}

impl RepaymentEdge {
    /// Returns the stable tagged-union selector.
    #[must_use]
    pub const fn tag(self) -> RepaymentEdgeTag {
        match self {
            Self::None => RepaymentEdgeTag::None,
            Self::ObserverProjection { .. } => RepaymentEdgeTag::ObserverProjection,
            Self::PhysicalCompaction { .. } => RepaymentEdgeTag::PhysicalCompaction,
            Self::MarkerDelivery { .. } => RepaymentEdgeTag::MarkerDelivery,
            Self::ParticipantCursorProgress(_) => RepaymentEdgeTag::ParticipantCursorProgress,
            Self::DetachedCredentialRecovery { .. } => RepaymentEdgeTag::DetachedCredentialRecovery,
            Self::DetachedMarkerRelease { .. } => RepaymentEdgeTag::DetachedMarkerRelease,
            Self::DetachedCursorRelease { .. } => RepaymentEdgeTag::DetachedCursorRelease,
        }
    }
}

/// Exact common envelope alternatives for closure-checked operations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClosureCheckedEnvelope {
    /// Enrollment admission.
    Enrollment(EnrollmentEnvelope),
    /// Credential attach or supersession.
    CredentialAttach(AttachEnvelope),
    /// Live or detached terminal Leave.
    Leave(LeaveEnvelope),
    /// Ordinary record admission.
    RecordAdmission(RecordAdmissionEnvelope),
}

/// Unchanged-prestate suffix shared by every closure refusal scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClosureSnapshot {
    /// Identity slots currently owning marker capacity credits.
    pub marker_capacity_credits: u64,
    /// Live marker anchors.
    pub marker_anchors: u64,
    /// Entry debt.
    pub entry_debt: u64,
    /// Byte debt.
    pub byte_debt: u64,
    /// Current clear/edge state.
    pub repayment_edge: RepaymentEdge,
    /// Sequence claims owned by the current edge.
    pub edge_sequence_claims: u64,
    /// Admission-order position claims owned by the current edge.
    pub edge_order_position_claims: u64,
    /// Exact current edge recovery-claim occupancy.
    pub edge_k_remaining: ResourceVector,
    /// Exact componentwise `cap - B` headroom.
    pub k_headroom: WideResourceVector,
    /// Activated churn cycles already used.
    ///
    /// `docs/design/LP-EXTRACTION-GOAL.md` requires excluding the occurrence
    /// array. The frozen contract stores this counter as the same `u32` domain
    /// as `J`; this is the chosen wire interpretation where R-D1 is terse.
    pub episode_churn_used: u32,
    /// Churn cycles this transaction would add.
    pub delta_cycles: u64,
    /// Configured episode churn limit, interpreted in the stored `u32` domain.
    pub episode_churn_limit: u32,
}

/// Capacity-specific closure refusal suffix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClosureCapacityReason {
    /// First failing component.
    pub dimension: ResourceDimension,
    /// Simulated maximum required amount.
    pub required: u128,
    /// Configured component limit.
    pub limit: u128,
}

/// Exact closure refusal tagged body; no optional capacity field bag exists.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClosureRefusalReason {
    /// Componentwise entry or byte capacity failure.
    Capacity(ClosureCapacityReason),
    /// Recovery would violate the current detached edge fence.
    RecoveryFence,
    /// A delivered marker still awaits acknowledgement.
    DeliveredMarkerAwaitingAck,
    /// Optional lifecycle churn would exceed the episode limit.
    EpisodeChurnLimit,
}

/// Complete marker-closure capacity outcome payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarkerClosureCapacityExceeded {
    /// Exact triggering request envelope.
    pub request: ClosureCheckedEnvelope,
    /// Unchanged closure state disclosed by the outcome.
    pub snapshot: ClosureSnapshot,
    /// Selected exact scope and scope-specific suffix.
    pub reason: ClosureRefusalReason,
}
