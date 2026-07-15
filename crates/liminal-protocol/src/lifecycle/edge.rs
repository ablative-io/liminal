use crate::algebra::WideResourceVector;
use crate::wire::{BindingEpoch, DeliverySeq, ParticipantId};

/// Nonzero componentwise closure debt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClosureDebt(WideResourceVector);

impl ClosureDebt {
    /// Creates debt only when at least one component is nonzero.
    #[must_use]
    pub const fn new(value: WideResourceVector) -> Option<Self> {
        if value.is_zero() {
            None
        } else {
            Some(Self(value))
        }
    }

    /// Returns exact entry/byte debt.
    #[must_use]
    pub const fn value(self) -> WideResourceVector {
        self.0
    }
}

/// Observer-projection witness.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObserverProjection {
    /// Projection must complete through this sequence.
    pub through_seq: DeliverySeq,
}

/// Physical-compaction range witness.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PhysicalCompaction {
    /// Floor before required compaction.
    pub from_floor: DeliverySeq,
    /// Inclusive required compaction boundary.
    pub through_seq: DeliverySeq,
}

/// Exact marker delivery witness.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MarkerDelivery {
    /// Affected participant.
    pub participant_id: ParticipantId,
    /// Binding epoch that must receive the marker.
    pub binding_epoch: BindingEpoch,
    /// Marker sequence.
    pub marker_delivery_seq: DeliverySeq,
}

/// Continuous cursor-progress witness with no delivered marker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CursorProgressContinuous {
    /// Affected participant.
    pub participant_id: ParticipantId,
    /// Binding epoch owning the cursor witness.
    pub binding_epoch: BindingEpoch,
    /// Required cumulative boundary.
    pub through_seq: DeliverySeq,
}

/// Marker-backed cursor-progress witness.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CursorProgressMarker {
    /// Affected participant.
    pub participant_id: ParticipantId,
    /// Binding epoch that received the marker.
    pub binding_epoch: BindingEpoch,
    /// Required cumulative boundary.
    pub through_seq: DeliverySeq,
    /// Exact delivered marker.
    pub marker_delivery_seq: DeliverySeq,
}

/// Cursor progress split into typestates rather than an optional marker bag.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParticipantCursorProgress {
    /// Continuous cursor witness.
    Continuous(CursorProgressContinuous),
    /// Exact marker acknowledgement witness.
    Marker(CursorProgressMarker),
}

/// Detached fenced credential-recovery witness.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DetachedCredentialRecovery {
    /// Detached participant.
    pub participant_id: ParticipantId,
    /// Delivered marker anchoring recovery.
    pub marker_delivery_seq: DeliverySeq,
    /// Prior dead binding epoch.
    pub prior_binding_epoch: BindingEpoch,
}

/// Leave-only undelivered-marker release witness.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DetachedMarkerRelease {
    /// Detached participant.
    pub participant_id: ParticipantId,
    /// Undelivered marker anchor.
    pub marker_delivery_seq: DeliverySeq,
    /// Last dead binding epoch.
    pub last_dead_binding_epoch: BindingEpoch,
}

/// Leave-only detached-cursor release witness.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DetachedCursorRelease {
    /// Detached participant.
    pub participant_id: ParticipantId,
    /// Last dead binding epoch.
    pub last_dead_binding_epoch: BindingEpoch,
}

/// Exact seven non-clear stored edge kinds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoredEdge {
    /// Observer projection.
    ObserverProjection(ObserverProjection),
    /// Physical compaction.
    PhysicalCompaction(PhysicalCompaction),
    /// Marker delivery.
    MarkerDelivery(MarkerDelivery),
    /// Participant cursor progress.
    ParticipantCursorProgress(ParticipantCursorProgress),
    /// Detached credential recovery.
    DetachedCredentialRecovery(DetachedCredentialRecovery),
    /// Detached marker release.
    DetachedMarkerRelease(DetachedMarkerRelease),
    /// Detached cursor release.
    DetachedCursorRelease(DetachedCursorRelease),
}

/// Closure state makes a clear edge with nonzero debt unconstructible.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClosureState {
    /// No edge and zero debt.
    Clear,
    /// Nonzero debt paired with one exact stored witness.
    Owed {
        /// Exact nonzero debt.
        debt: ClosureDebt,
        /// Current repayment witness.
        edge: StoredEdge,
    },
}

/// Successor set after debt completion or detached Leave.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DebtCompletion {
    /// Debt cleared.
    Clear,
    /// Observer projection remains.
    ObserverProjection(ObserverProjection),
    /// Physical compaction remains.
    PhysicalCompaction(PhysicalCompaction),
}

/// OP/PC completion successor set; direct DCR is absent by construction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectionCompactionSuccessor {
    /// Debt cleared.
    Clear,
    /// Later observer projection.
    ObserverProjection(ObserverProjection),
    /// Later physical compaction.
    PhysicalCompaction(PhysicalCompaction),
    /// Marker delivery.
    MarkerDelivery(MarkerDelivery),
    /// Participant cursor progress.
    ParticipantCursorProgress(ParticipantCursorProgress),
    /// Detached marker release.
    DetachedMarkerRelease(DetachedMarkerRelease),
    /// Detached cursor release.
    DetachedCursorRelease(DetachedCursorRelease),
}

impl ObserverProjection {
    /// Applies projection and atomically selects its typed successor.
    #[must_use]
    pub const fn complete(
        self,
        successor: ProjectionCompactionSuccessor,
    ) -> ProjectionCompactionSuccessor {
        let _ = self;
        successor
    }
}

impl PhysicalCompaction {
    /// Applies compaction and atomically selects its typed successor.
    #[must_use]
    pub const fn complete(
        self,
        successor: ProjectionCompactionSuccessor,
    ) -> ProjectionCompactionSuccessor {
        let _ = self;
        successor
    }
}

impl MarkerDelivery {
    /// Final-emitter delivery becomes exact marker-backed cursor progress.
    #[must_use]
    pub const fn delivered(self, through_seq: DeliverySeq) -> CursorProgressMarker {
        CursorProgressMarker {
            participant_id: self.participant_id,
            binding_epoch: self.binding_epoch,
            through_seq,
            marker_delivery_seq: self.marker_delivery_seq,
        }
    }

    /// Binding fate before delivery becomes Leave-only detached marker release.
    #[must_use]
    pub const fn binding_fate(self) -> DetachedMarkerRelease {
        DetachedMarkerRelease {
            participant_id: self.participant_id,
            marker_delivery_seq: self.marker_delivery_seq,
            last_dead_binding_epoch: self.binding_epoch,
        }
    }

    /// Charged supersession retargets exact delivery to a new binding epoch.
    #[must_use]
    pub const fn retarget(self, binding_epoch: BindingEpoch) -> Self {
        Self {
            binding_epoch,
            ..self
        }
    }
}

/// Fate successor selected solely from cursor-progress typestate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CursorFateSuccessor {
    /// Marker-backed state becomes fenced detached recovery.
    DetachedCredentialRecovery(DetachedCredentialRecovery),
    /// Continuous state becomes Leave-only cursor release.
    DetachedCursorRelease(DetachedCursorRelease),
}

impl ParticipantCursorProgress {
    /// Completion can select only clear, observer projection, or compaction.
    #[must_use]
    pub const fn complete(self, successor: DebtCompletion) -> DebtCompletion {
        let _ = self;
        successor
    }

    /// Binding fate selects DCR only from marker-backed state and `DCursor` only
    /// from continuous state.
    #[must_use]
    pub const fn binding_fate(self) -> CursorFateSuccessor {
        match self {
            Self::Continuous(value) => {
                CursorFateSuccessor::DetachedCursorRelease(DetachedCursorRelease {
                    participant_id: value.participant_id,
                    last_dead_binding_epoch: value.binding_epoch,
                })
            }
            Self::Marker(value) => {
                CursorFateSuccessor::DetachedCredentialRecovery(DetachedCredentialRecovery {
                    participant_id: value.participant_id,
                    marker_delivery_seq: value.marker_delivery_seq,
                    prior_binding_epoch: value.binding_epoch,
                })
            }
        }
    }
}

impl CursorProgressContinuous {
    /// Charged supersession retargets a continuous cursor witness.
    #[must_use]
    pub const fn retarget(self, binding_epoch: BindingEpoch) -> Self {
        Self {
            binding_epoch,
            ..self
        }
    }
}

/// Detached attach refusal enforced by stored-edge typestate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DetachedAttachRefusal {
    /// Ordinary attach violates the recovery fence.
    RecoveryFence,
    /// Presented marker was never delivered.
    MarkerNotDelivered,
    /// Edge owns no matching marker.
    MarkerMismatch,
    /// Marker-backed PCP must be acknowledged before supersession.
    DeliveredMarkerAwaitingAck,
}

impl CursorProgressMarker {
    /// Supersession is always refused until the delivered marker is accepted.
    #[must_use]
    pub const fn supersession_refusal(self) -> DetachedAttachRefusal {
        let _ = self;
        DetachedAttachRefusal::DeliveredMarkerAwaitingAck
    }
}

/// Evidence for the sole successful participant transition out of a Leave-only edge.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KClaimBackedDetachedLeave {
    participant_id: ParticipantId,
}

impl KClaimBackedDetachedLeave {
    /// Creates evidence after authority and K-claim verification.
    #[must_use]
    pub const fn verified(participant_id: ParticipantId) -> Self {
        Self { participant_id }
    }
}

mod sealed {
    pub trait Sealed {}
}

/// Sealed intended-dead-end contract for DMR and `DCursor`.
pub trait LeaveOnlyEdge: sealed::Sealed + Sized {
    /// Sole successful participant successor: exact-current K-backed detached Leave.
    ///
    /// # Errors
    ///
    /// Returns the unchanged edge when evidence names another participant.
    fn leave(
        self,
        evidence: KClaimBackedDetachedLeave,
        successor: DebtCompletion,
    ) -> Result<DebtCompletion, Self>;
}

impl sealed::Sealed for DetachedMarkerRelease {}
impl sealed::Sealed for DetachedCursorRelease {}

impl LeaveOnlyEdge for DetachedMarkerRelease {
    fn leave(
        self,
        evidence: KClaimBackedDetachedLeave,
        successor: DebtCompletion,
    ) -> Result<DebtCompletion, Self> {
        if evidence.participant_id == self.participant_id {
            Ok(successor)
        } else {
            Err(self)
        }
    }
}

impl LeaveOnlyEdge for DetachedCursorRelease {
    fn leave(
        self,
        evidence: KClaimBackedDetachedLeave,
        successor: DebtCompletion,
    ) -> Result<DebtCompletion, Self> {
        if evidence.participant_id == self.participant_id {
            Ok(successor)
        } else {
            Err(self)
        }
    }
}

impl DetachedCredentialRecovery {
    /// Fenced attach transfers K-backed charge and selects only clear/OP/PC.
    #[must_use]
    pub const fn fenced_attach(self, successor: DebtCompletion) -> DebtCompletion {
        let _ = self;
        successor
    }

    /// K-backed detached Leave selects only clear/OP/PC.
    #[must_use]
    pub const fn detached_leave(self, successor: DebtCompletion) -> DebtCompletion {
        let _ = self;
        successor
    }

    /// Ordinary non-fenced attach is refused by `RecoveryFence`.
    #[must_use]
    pub const fn ordinary_attach_refusal(self) -> DetachedAttachRefusal {
        let _ = self;
        DetachedAttachRefusal::RecoveryFence
    }
}

impl DetachedMarkerRelease {
    /// Ordinary attach cannot cross this Leave-only edge.
    #[must_use]
    pub const fn ordinary_attach_refusal(self) -> DetachedAttachRefusal {
        let _ = self;
        DetachedAttachRefusal::RecoveryFence
    }

    /// Presenting the undelivered marker is explicitly `MarkerNotDelivered`.
    #[must_use]
    pub const fn marker_attach_refusal(self) -> DetachedAttachRefusal {
        let _ = self;
        DetachedAttachRefusal::MarkerNotDelivered
    }
}

impl DetachedCursorRelease {
    /// Attach without a marker cannot cross this Leave-only edge.
    #[must_use]
    pub const fn ordinary_attach_refusal(self) -> DetachedAttachRefusal {
        let _ = self;
        DetachedAttachRefusal::RecoveryFence
    }

    /// Presenting any marker mismatches a cursor-only edge.
    #[must_use]
    pub const fn marker_attach_refusal(self) -> DetachedAttachRefusal {
        let _ = self;
        DetachedAttachRefusal::MarkerMismatch
    }
}

/// Typed completion events retained after excluding occurrence-array storage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Event {
    /// Observer projection completed.
    ProjectionCompleted {
        /// Completed projection boundary.
        through_seq: DeliverySeq,
    },
    /// Physical compaction completed.
    CompactionCompleted {
        /// Completed range start.
        from_floor: DeliverySeq,
        /// Completed range end.
        through_seq: DeliverySeq,
    },
    /// Marker append completed.
    MarkerAppended {
        /// Appended marker sequence.
        marker_delivery_seq: DeliverySeq,
    },
    /// Marker delivery completed.
    MarkerDelivered {
        /// Recipient participant.
        participant_id: ParticipantId,
        /// Delivered marker sequence.
        marker_delivery_seq: DeliverySeq,
    },
    /// Participant-scoped cursor progress completed.
    CursorProgressed {
        /// Participant index.
        participant_index: ParticipantId,
        /// Committed boundary.
        boundary: DeliverySeq,
    },
    /// Binding fate was observed.
    BindingFateObserved {
        /// Affected participant.
        participant_id: ParticipantId,
    },
    /// Fenced recovery committed.
    FencedRecoveryCommitted {
        /// Recovered participant.
        participant_id: ParticipantId,
    },
    /// Terminal Leave committed.
    LeaveCommitted {
        /// Retired participant.
        participant_id: ParticipantId,
    },
}
