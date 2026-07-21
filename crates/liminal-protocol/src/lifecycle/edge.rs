use alloc::boxed::Box;

use crate::algebra::{ResourceVector, WideResourceVector};
use crate::wire::{BindingEpoch, ConversationId, DeliverySeq, ParticipantId, ParticipantIndex};

use super::{
    ActiveBinding, CommittedDiedTerminal, ObserverProgressProjection,
    claim_frontier::{ValidatedMarkerCandidate, ValidatedMarkerRecord},
};

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
    through_seq: DeliverySeq,
}

impl ObserverProjection {
    /// Creates an exact observer-projection witness.
    #[must_use]
    pub const fn new(through_seq: DeliverySeq) -> Self {
        Self { through_seq }
    }

    /// Returns the exact projection boundary.
    #[must_use]
    pub const fn through_seq(self) -> DeliverySeq {
        self.through_seq
    }
}

/// Physical-compaction range witness.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PhysicalCompaction {
    from_floor: DeliverySeq,
    through_seq: DeliverySeq,
}

impl PhysicalCompaction {
    /// Creates a nonempty, ordered compaction range.
    #[must_use]
    pub const fn new(from_floor: DeliverySeq, through_seq: DeliverySeq) -> Option<Self> {
        if from_floor <= through_seq {
            Some(Self {
                from_floor,
                through_seq,
            })
        } else {
            None
        }
    }

    /// Returns the exact first sequence in the compaction range.
    #[must_use]
    pub const fn from_floor(self) -> DeliverySeq {
        self.from_floor
    }

    /// Returns the exact inclusive compaction boundary.
    #[must_use]
    pub const fn through_seq(self) -> DeliverySeq {
        self.through_seq
    }
}

/// Exact marker-delivery witness.
///
/// This witness has no public constructor. Fresh delivery is produced only by
/// the claim frontier's consuming marker-drain transition; cold restoration
/// requires its sealed retained-marker-record authority. Raw participant,
/// binding, and sequence values therefore cannot create recovery authority.
///
/// ```compile_fail
/// use liminal_protocol::{
///     lifecycle::MarkerDelivery,
///     wire::{BindingEpoch, ConnectionIncarnation, Generation},
/// };
///
/// let epoch = BindingEpoch::new(
///     ConnectionIncarnation::new(1, 1),
///     Generation::ONE,
/// );
/// let _ = MarkerDelivery::new(7, epoch, 11);
/// ```
// The frozen tag spells this required field `marker_delivery_seq`.
#[allow(clippy::struct_field_names)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MarkerDelivery {
    conversation_id: ConversationId,
    participant_id: ParticipantId,
    binding_epoch: BindingEpoch,
    marker_delivery_seq: DeliverySeq,
}

impl MarkerDelivery {
    /// Creates the exact post-append marker successor only from one
    /// frontier-consumed marker candidate.
    ///
    /// A candidate whose target epoch is still bound selects live delivery. A
    /// candidate whose target epoch has already died selects the undelivered
    /// detached release directly, so no transient live-delivery authority is
    /// fabricated for a detached participant.
    #[must_use]
    pub(super) const fn successor_from_validated_candidate(
        candidate: ValidatedMarkerCandidate,
    ) -> StoredEdge {
        let conversation_id = candidate.conversation_id();
        let participant_id = candidate.participant_id();
        let marker_delivery_seq = candidate.delivery_seq();
        let successor = match candidate.target_binding() {
            super::FrontierBinding::Bound(binding_epoch) => StoredEdge::MarkerDelivery(Self {
                conversation_id,
                participant_id,
                binding_epoch,
                marker_delivery_seq,
            }),
            super::FrontierBinding::Detached(last_dead_binding_epoch) => {
                StoredEdge::DetachedMarkerRelease(DetachedMarkerRelease {
                    participant_id,
                    marker_delivery_seq,
                    last_dead_binding_epoch,
                })
            }
        };
        candidate.consume();
        successor
    }

    /// Rebuilds delivery only from one frontier-validated retained marker.
    #[must_use]
    pub(super) const fn from_validated_record(record: &ValidatedMarkerRecord) -> Self {
        Self {
            conversation_id: record.conversation_id(),
            participant_id: record.participant_id(),
            binding_epoch: record.binding_epoch(),
            marker_delivery_seq: record.delivery_seq(),
        }
    }

    /// Returns the conversation whose frontier authority minted this delivery.
    #[must_use]
    pub const fn conversation_id(self) -> ConversationId {
        self.conversation_id
    }

    /// Returns the marker owner.
    #[must_use]
    pub const fn participant_id(self) -> ParticipantId {
        self.participant_id
    }

    /// Returns the exact delivery epoch.
    #[must_use]
    pub const fn binding_epoch(self) -> BindingEpoch {
        self.binding_epoch
    }

    /// Returns the exact marker sequence.
    #[must_use]
    pub const fn marker_delivery_seq(self) -> DeliverySeq {
        self.marker_delivery_seq
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::too_many_lines)]
pub fn validated_marker_record_for_test(
    conversation_id: crate::wire::ConversationId,
    participant_id: ParticipantId,
    target_binding: super::claim_frontier::FrontierBinding,
    marker_delivery_seq: DeliverySeq,
    cursor: DeliverySeq,
) -> ValidatedMarkerRecord {
    use alloc::{vec, vec::Vec};

    use crate::outcome::CandidatePhase;

    use super::{
        AdmissionOrder, OrderClaims, OrderHigh, OrderLedger, RecoverySequenceReserve,
        SequenceClaims, SequenceLedger,
        claim_frontier::{
            BindingTerminalOwner, ClaimFrontiers, ClaimFrontiersRestore, FrontierBinding,
            FrontierParticipant, ImmutableSequenceCandidate, MarkerProvenance, MarkerRecordRequest,
            MovableOrderClaim, MovableSequenceClaim, OrderClaimFrontierRestore, OrderDirectOwner,
            RetainedCausalRecord, RetainedCausalRecordKind, SequenceClaimFrontierRestore,
            SequenceDirectOwner, SequenceProductRangesRestore, TerminalProductRangeRestore,
        },
    };

    let identity_slot_limit = participant_id
        .checked_add(1)
        .expect("test participant must fit a half-open identity domain");
    let exit_seq = marker_delivery_seq
        .checked_add(1)
        .expect("test marker must leave an exit-claim suffix");
    let binding_epoch = match target_binding {
        FrontierBinding::Bound(epoch) | FrontierBinding::Detached(epoch) => epoch,
    };
    let terminal_owner = BindingTerminalOwner {
        participant_index: participant_id,
        binding_epoch,
    };
    let (sequence_claims, sequence_movable, products, order_claims, order_movable) =
        match target_binding {
            FrontierBinding::Bound(_) => {
                let terminal_seq = exit_seq
                    .checked_add(1)
                    .expect("test marker must leave a terminal-claim suffix");
                let product_seq = terminal_seq
                    .checked_add(1)
                    .expect("test marker must leave a terminal-product suffix");
                (
                    SequenceClaims::new(1, 1, 0, RecoverySequenceReserve::None),
                    vec![
                        MovableSequenceClaim {
                            delivery_seq: exit_seq,
                            owner: SequenceDirectOwner::MembershipExit {
                                participant_index: participant_id,
                            },
                        },
                        MovableSequenceClaim {
                            delivery_seq: terminal_seq,
                            owner: SequenceDirectOwner::BindingTerminal(terminal_owner),
                        },
                    ],
                    SequenceProductRangesRestore {
                        live_times_terminal: vec![TerminalProductRangeRestore {
                            start: product_seq,
                            length: 1,
                            terminal: terminal_owner,
                        }],
                        live_times_replacement_terminal: None,
                        other_live_times_exit: vec![],
                    },
                    OrderClaims::new(1, 1, false, false)
                        .expect("bound test claims have no torn recovery pair"),
                    vec![
                        MovableOrderClaim {
                            transaction_order: 1,
                            owner: OrderDirectOwner::ActiveBindingTerminal(terminal_owner),
                        },
                        MovableOrderClaim {
                            transaction_order: 2,
                            owner: OrderDirectOwner::MembershipExit {
                                participant_index: participant_id,
                            },
                        },
                    ],
                )
            }
            FrontierBinding::Detached(_) => (
                SequenceClaims::new(1, 0, 0, RecoverySequenceReserve::None),
                vec![MovableSequenceClaim {
                    delivery_seq: exit_seq,
                    owner: SequenceDirectOwner::MembershipExit {
                        participant_index: participant_id,
                    },
                }],
                SequenceProductRangesRestore::default(),
                OrderClaims::new(0, 1, false, false)
                    .expect("detached test claims have no torn recovery pair"),
                vec![MovableOrderClaim {
                    transaction_order: 1,
                    owner: OrderDirectOwner::MembershipExit {
                        participant_index: participant_id,
                    },
                }],
            ),
        };
    let sequence_ledger = SequenceLedger::try_new(marker_delivery_seq, sequence_claims)
        .expect("test sequence frontier is within the numeric suffix");
    let order_ledger = OrderLedger::try_new(OrderHigh::Allocated(0), order_claims)
        .expect("test order frontier is within the numeric suffix");
    let admission_order = AdmissionOrder::new(0, CandidatePhase::CompactionMarker, participant_id);
    let mut prevalidated = ClaimFrontiers::prevalidate(
        ClaimFrontiersRestore {
            conversation_id,
            active_identities: vec![FrontierParticipant::new(
                participant_id,
                cursor,
                target_binding,
            )],
            identity_slot_limit,
            retained_floor: u128::from(marker_delivery_seq),
            retained_record_limit: 1,
            retained_records: vec![RetainedCausalRecord {
                delivery_seq: marker_delivery_seq,
                admission_order,
                kind: RetainedCausalRecordKind::CompactionMarker {
                    participant_index: participant_id,
                    provenance: MarkerProvenance::NonProductM,
                },
            }],
            active_marker_anchors: vec![marker_delivery_seq],
            historical_marker_deliveries: vec![],
            historical_causal_facts: vec![],
            sequence: SequenceClaimFrontierRestore {
                movable_claims: sequence_movable,
                immutable_candidates: Vec::<ImmutableSequenceCandidate>::new(),
                products,
                recovery: None,
            },
            order: OrderClaimFrontierRestore {
                movable_claims: order_movable,
                immutable_candidates: vec![],
                recovery: None,
            },
            recovery_marker_delivery_seq: None,
        },
        sequence_ledger,
        order_ledger,
    )
    .expect("complete test claim frontier must prevalidate");
    let record = prevalidated
        .take_marker_record(MarkerRecordRequest::planned(
            participant_id,
            marker_delivery_seq,
            target_binding,
        ))
        .expect("prevalidated test frontier retains its exact marker");
    if cursor >= marker_delivery_seq {
        record.delivered_for_test()
    } else {
        record
    }
}

#[cfg(test)]
pub fn marker_delivery_for_test(
    participant_id: ParticipantId,
    binding_epoch: BindingEpoch,
    marker_delivery_seq: DeliverySeq,
) -> Result<MarkerDelivery, super::storage::StorageRestoreError> {
    let record = validated_marker_record_for_test(
        1,
        participant_id,
        super::claim_frontier::FrontierBinding::Bound(binding_epoch),
        marker_delivery_seq,
        marker_delivery_seq.saturating_sub(1),
    );
    super::storage::MarkerDeliveryRestore {
        participant_id,
        binding_epoch,
        marker_delivery_seq,
    }
    .restore_bound(1, record)
}

/// Continuous cursor-progress witness with no delivered marker.
///
/// This witness deliberately has no public constructor. A caller outside this
/// crate cannot turn raw participant/epoch values into executable binding-fate
/// authority; recovered-epoch fate must instead originate from
/// `FencedAttachCommit::recovered_binding_fate`.
///
/// ```compile_fail
/// use liminal_protocol::{
///     lifecycle::CursorProgressContinuous,
///     wire::BindingEpoch,
/// };
///
/// fn fabricate(epoch: BindingEpoch) {
///     let _ = CursorProgressContinuous::new(7, epoch, 11);
/// }
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CursorProgressContinuous {
    participant_id: ParticipantId,
    binding_epoch: BindingEpoch,
    through_seq: DeliverySeq,
}

impl CursorProgressContinuous {
    /// Creates an exact current-epoch continuous-cursor witness internally.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn new(
        participant_id: ParticipantId,
        binding_epoch: BindingEpoch,
        through_seq: DeliverySeq,
    ) -> Self {
        Self {
            participant_id,
            binding_epoch,
            through_seq,
        }
    }

    /// Returns the participant whose cursor is required.
    #[must_use]
    pub const fn participant_id(self) -> ParticipantId {
        self.participant_id
    }

    /// Returns the exact binding epoch.
    #[must_use]
    pub const fn binding_epoch(self) -> BindingEpoch {
        self.binding_epoch
    }

    /// Returns the required cumulative boundary.
    #[must_use]
    pub const fn through_seq(self) -> DeliverySeq {
        self.through_seq
    }
}

/// Marker-backed cursor-progress witness.
///
/// This value has no public constructor. It is produced only by consuming an
/// exact [`MarkerDelivery`] with its matching [`Event::marker_delivered`]. That
/// makes the durable exact-epoch delivery fact required by the frozen contract
/// a type-level precondition for detached credential recovery.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CursorProgressMarker {
    conversation_id: ConversationId,
    participant_id: ParticipantId,
    binding_epoch: BindingEpoch,
    through_seq: DeliverySeq,
    marker_delivery_seq: DeliverySeq,
}

impl CursorProgressMarker {
    /// Returns the conversation inherited from the exact marker delivery.
    #[must_use]
    pub const fn conversation_id(self) -> ConversationId {
        self.conversation_id
    }

    /// Returns the participant whose marker must be accepted.
    #[must_use]
    pub const fn participant_id(self) -> ParticipantId {
        self.participant_id
    }

    /// Returns the exact epoch that received the marker.
    #[must_use]
    pub const fn binding_epoch(self) -> BindingEpoch {
        self.binding_epoch
    }

    /// Returns the required cumulative boundary.
    #[must_use]
    pub const fn through_seq(self) -> DeliverySeq {
        self.through_seq
    }

    /// Returns the exact delivered marker.
    #[must_use]
    pub const fn marker_delivery_seq(self) -> DeliverySeq {
        self.marker_delivery_seq
    }
}

/// Cursor progress split into typestates rather than an optional marker bag.
///
/// Continuous construction is crate-private so matching raw participant and
/// epoch values cannot fabricate `DetachedCursorRelease` authority.
///
/// ```compile_fail
/// use liminal_protocol::{
///     lifecycle::ParticipantCursorProgress,
///     wire::BindingEpoch,
/// };
///
/// fn fabricate(epoch: BindingEpoch) {
///     let _ = ParticipantCursorProgress::continuous(7, epoch, 11);
/// }
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParticipantCursorProgress {
    /// Continuous cursor witness.
    Continuous(CursorProgressContinuous),
    /// Exact marker acknowledgement witness, derivable only from delivery.
    Marker(CursorProgressMarker),
}

impl ParticipantCursorProgress {
    /// Creates a continuous, no-marker cursor witness internally.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn continuous(
        participant_id: ParticipantId,
        binding_epoch: BindingEpoch,
        through_seq: DeliverySeq,
    ) -> Self {
        Self::Continuous(CursorProgressContinuous::new(
            participant_id,
            binding_epoch,
            through_seq,
        ))
    }

    pub(super) fn restore_continuous(
        authority: OrdinaryBindingAuthority,
        participant_id: ParticipantId,
        binding_epoch: BindingEpoch,
        through_seq: DeliverySeq,
    ) -> Option<Self> {
        if authority.binding.participant_id != participant_id
            || authority.binding.binding_epoch != binding_epoch
            || authority.through_seq != through_seq
        {
            return None;
        }
        Some(Self::Continuous(CursorProgressContinuous {
            participant_id,
            binding_epoch,
            through_seq,
        }))
    }

    /// Returns the participant whose cursor is required.
    #[must_use]
    pub const fn participant_id(self) -> ParticipantId {
        match self {
            Self::Continuous(value) => value.participant_id,
            Self::Marker(value) => value.participant_id,
        }
    }

    /// Returns the exact binding epoch.
    #[must_use]
    pub const fn binding_epoch(self) -> BindingEpoch {
        match self {
            Self::Continuous(value) => value.binding_epoch,
            Self::Marker(value) => value.binding_epoch,
        }
    }

    /// Returns the required cumulative boundary.
    #[must_use]
    pub const fn through_seq(self) -> DeliverySeq {
        match self {
            Self::Continuous(value) => value.through_seq,
            Self::Marker(value) => value.through_seq,
        }
    }

    /// Returns the exact delivered marker when this is marker-backed.
    #[must_use]
    pub const fn marker_delivery_seq(self) -> Option<DeliverySeq> {
        match self {
            Self::Continuous(_) => None,
            Self::Marker(value) => Some(value.marker_delivery_seq),
        }
    }
}

/// Detached fenced credential-recovery witness.
///
/// This state is produced only by the exact binding fate of a marker-backed
/// cursor witness; callers cannot fabricate a durable delivery fact.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DetachedCredentialRecovery {
    conversation_id: ConversationId,
    participant_id: ParticipantId,
    marker_delivery_seq: DeliverySeq,
    prior_binding_epoch: BindingEpoch,
}

impl DetachedCredentialRecovery {
    /// Rebuilds the harmless, copyable recovery description after the storage
    /// layer has validated its complete terminal and marker-progress audit.
    ///
    /// Marker-occurrence authority is deliberately not part of this value; the
    /// move-only frontier owner must still consume its private validated record
    /// before a fenced proof can be minted.
    pub(super) const fn from_storage_description(
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        marker_delivery_seq: DeliverySeq,
        prior_binding_epoch: BindingEpoch,
    ) -> Self {
        Self {
            conversation_id,
            participant_id,
            marker_delivery_seq,
            prior_binding_epoch,
        }
    }

    /// Returns the conversation inherited from the marker-backed cursor witness.
    #[must_use]
    pub const fn conversation_id(self) -> ConversationId {
        self.conversation_id
    }

    /// Returns the detached participant.
    #[must_use]
    pub const fn participant_id(self) -> ParticipantId {
        self.participant_id
    }

    /// Returns the delivered recovery marker.
    #[must_use]
    pub const fn marker_delivery_seq(self) -> DeliverySeq {
        self.marker_delivery_seq
    }

    /// Returns the prior authoritative epoch.
    #[must_use]
    pub const fn prior_binding_epoch(self) -> BindingEpoch {
        self.prior_binding_epoch
    }
}

#[cfg(test)]
pub fn validated_marker_record_for_recovery_test(
    recovery: DetachedCredentialRecovery,
) -> ValidatedMarkerRecord {
    validated_marker_record_for_test(
        recovery.conversation_id(),
        recovery.participant_id(),
        super::claim_frontier::FrontierBinding::Detached(recovery.prior_binding_epoch()),
        recovery.marker_delivery_seq(),
        recovery.marker_delivery_seq(),
    )
}

/// Leave-only undelivered-marker release witness.
///
/// This state is produced only when exact binding fate consumes a marker that
/// has not reached [`CursorProgressMarker`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DetachedMarkerRelease {
    participant_id: ParticipantId,
    marker_delivery_seq: DeliverySeq,
    last_dead_binding_epoch: BindingEpoch,
}

impl DetachedMarkerRelease {
    /// Returns the detached participant.
    #[must_use]
    pub const fn participant_id(self) -> ParticipantId {
        self.participant_id
    }

    /// Returns the undelivered marker.
    #[must_use]
    pub const fn marker_delivery_seq(self) -> DeliverySeq {
        self.marker_delivery_seq
    }

    /// Returns the dead binding epoch.
    #[must_use]
    pub const fn last_dead_binding_epoch(self) -> BindingEpoch {
        self.last_dead_binding_epoch
    }
}

/// Leave-only detached-cursor release witness.
///
/// This state is produced only when exact binding fate consumes a continuous
/// cursor witness with no marker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DetachedCursorRelease {
    participant_id: ParticipantId,
    last_dead_binding_epoch: BindingEpoch,
}

impl DetachedCursorRelease {
    /// Returns the detached participant.
    #[must_use]
    pub const fn participant_id(self) -> ParticipantId {
        self.participant_id
    }

    /// Returns the dead binding epoch.
    #[must_use]
    pub const fn last_dead_binding_epoch(self) -> BindingEpoch {
        self.last_dead_binding_epoch
    }
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

/// Opaque proof that ordinary detached attach entered from a legal closure state.
///
/// Only [`ClosureState::ordinary_detached_attach_admission`] constructs this
/// value. Recovery-fenced DCR, DMR, and `DCursor` states therefore cannot enter
/// the ordinary detached-attach path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrdinaryDetachedAttachAdmission {
    _private: (),
}

impl ClosureState {
    /// Admits ordinary detached attach only from clear closure state.
    ///
    /// # Errors
    ///
    /// Returns the unchanged owed state for every stored edge, including DCR,
    /// DMR, and `DCursor`.
    pub const fn ordinary_detached_attach_admission(
        self,
    ) -> Result<OrdinaryDetachedAttachAdmission, Self> {
        match self {
            Self::Clear => Ok(OrdinaryDetachedAttachAdmission { _private: () }),
            Self::Owed { .. } => Err(self),
        }
    }
}

/// Validated completion restricted to clear, observer projection, or physical
/// compaction.
///
/// Fields are private so DCR, marker delivery, PCP, DMR, and `DCursor` cannot be
/// smuggled through a detached attach or Leave completion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DebtCompletion(ClosureState);

impl DebtCompletion {
    /// Selects the only legal edge-free state.
    #[must_use]
    pub const fn clear() -> Self {
        Self(ClosureState::Clear)
    }

    /// Selects an independent observer-projection successor under nonzero debt.
    #[must_use]
    pub const fn observer_projection(debt: ClosureDebt, edge: ObserverProjection) -> Self {
        Self(ClosureState::Owed {
            debt,
            edge: StoredEdge::ObserverProjection(edge),
        })
    }

    /// Selects an independent physical-compaction successor under nonzero debt.
    #[must_use]
    pub const fn physical_compaction(debt: ClosureDebt, edge: PhysicalCompaction) -> Self {
        Self(ClosureState::Owed {
            debt,
            edge: StoredEdge::PhysicalCompaction(edge),
        })
    }

    /// Returns the validated closure state.
    #[must_use]
    pub const fn into_state(self) -> ClosureState {
        self.0
    }
}

/// Opaque authority for the current epoch produced by an ordinary attach.
///
/// Only a successful non-fenced attach commit can construct this value. It is
/// therefore disjoint from [`FencedAttachCommit`]: a recovered binding cannot
/// use the ordinary no-marker fate path.
///
/// ```compile_fail
/// use liminal_protocol::lifecycle::{ActiveBinding, OrdinaryBindingAuthority};
///
/// fn fabricate(binding: ActiveBinding) {
///     let _ = OrdinaryBindingAuthority::new(binding, 11);
/// }
/// ```
///
/// An ordinary-attach fork also cannot extract authority through the public
/// surface. Only the protocol-owned aggregate/replay path may consume it:
///
/// ```compile_fail
/// use liminal_protocol::lifecycle::AttachCommit;
///
/// fn splice<F, V>(commit: &AttachCommit<F, V>) {
///     let _ = commit.ordinary_binding_authority();
/// }
/// ```
///
/// Even code handed the opaque type cannot execute its fate transition:
///
/// ```compile_fail
/// use liminal_protocol::lifecycle::{CommittedDiedTerminal, OrdinaryBindingAuthority};
///
/// fn execute(authority: OrdinaryBindingAuthority, terminal: CommittedDiedTerminal) {
///     let _ = authority.binding_fate(terminal, 11);
/// }
/// ```
///
/// ```compile_fail
/// use liminal_protocol::lifecycle::{Event, OrdinaryBindingAuthority};
///
/// fn advance(authority: OrdinaryBindingAuthority, event: Event) {
///     let _ = authority.cursor_progressed(event);
/// }
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrdinaryBindingAuthority {
    binding: ActiveBinding,
    through_seq: DeliverySeq,
}

impl OrdinaryBindingAuthority {
    pub(crate) const fn new(binding: ActiveBinding, through_seq: DeliverySeq) -> Self {
        Self {
            binding,
            through_seq,
        }
    }

    /// Returns the exact authoritative binding committed by ordinary attach.
    #[must_use]
    pub const fn binding(self) -> ActiveBinding {
        self.binding
    }

    /// Returns the durable no-marker cursor carried through ordinary attach.
    #[must_use]
    pub const fn through_seq(self) -> DeliverySeq {
        self.through_seq
    }

    /// Advances this ordinary binding's cursor through one exact normal ack.
    ///
    /// The returned authority preserves its attach provenance while replacing
    /// the cursor only when participant, epoch, and previous boundary all match.
    ///
    /// # Errors
    ///
    /// Returns this authority unchanged for another event class, participant,
    /// epoch, or previous cursor.
    pub(crate) fn cursor_progressed(self, event: Event) -> Result<Self, Self> {
        let EventKind::CursorProgressed {
            participant_id,
            binding_epoch,
            progress:
                CursorProgressEvent::Normal {
                    previous_cursor,
                    through_seq,
                },
            ..
        } = event.0
        else {
            return Err(self);
        };
        self.participant_ack_progressed(
            self.binding.conversation_id,
            participant_id,
            binding_epoch,
            previous_cursor,
            through_seq,
        )
    }

    /// Replays one protocol-selected normal acknowledgement into this authority.
    pub(crate) fn participant_ack_progressed(
        self,
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        binding_epoch: BindingEpoch,
        previous_cursor: DeliverySeq,
        through_seq: DeliverySeq,
    ) -> Result<Self, Self> {
        if conversation_id != self.binding.conversation_id
            || participant_id != self.binding.participant_id
            || binding_epoch != self.binding.binding_epoch
            || previous_cursor != self.through_seq
            || through_seq <= previous_cursor
        {
            return Err(self);
        }
        Ok(Self {
            through_seq,
            ..self
        })
    }

    /// Consumes the exact durable death of this ordinary binding.
    ///
    /// # Errors
    ///
    /// Returns this authority unchanged unless the terminal names the same
    /// participant, conversation, and binding epoch.
    pub(crate) fn binding_fate(
        self,
        terminal: CommittedDiedTerminal,
        resulting_floor: DeliverySeq,
    ) -> Result<OrdinaryBindingFate, Self> {
        if terminal.participant_id() != self.binding.participant_id
            || terminal.conversation_id() != self.binding.conversation_id
            || terminal.binding_epoch() != self.binding.binding_epoch
        {
            return Err(self);
        }
        Ok(OrdinaryBindingFate {
            conversation_id: self.binding.conversation_id,
            through_seq: self.through_seq,
            resulting_floor,
            release: DetachedCursorRelease {
                participant_id: self.binding.participant_id,
                last_dead_binding_epoch: self.binding.binding_epoch,
            },
        })
    }
}

/// Exact no-marker fate derived from an ordinary attach and its durable death.
///
/// Fields are private and the only public producer consumes an
/// [`AttachCommit`](crate::lifecycle::AttachCommit) carrying ordinary
/// provenance. A fenced attach cannot produce this type, so
/// executing it cannot bypass `FencedAttachCommit::recovered_binding_fate`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrdinaryBindingFate {
    conversation_id: ConversationId,
    through_seq: DeliverySeq,
    resulting_floor: DeliverySeq,
    release: DetachedCursorRelease,
}

impl OrdinaryBindingFate {
    /// Returns the conversation validated against the committed `Died` terminal.
    #[must_use]
    pub const fn conversation_id(self) -> ConversationId {
        self.conversation_id
    }

    /// Returns the durable cursor preceding the ordinary binding's death.
    #[must_use]
    pub const fn through_seq(self) -> DeliverySeq {
        self.through_seq
    }

    /// Returns the participant whose ordinary binding died.
    #[must_use]
    pub const fn participant_id(self) -> ParticipantId {
        self.release.participant_id
    }

    /// Returns the exact dead binding epoch whose fate was observed.
    #[must_use]
    pub const fn last_dead_binding_epoch(self) -> BindingEpoch {
        self.release.last_dead_binding_epoch
    }

    /// Returns the measured floor from the binding-fate transaction.
    #[must_use]
    pub const fn resulting_floor(self) -> DeliverySeq {
        self.resulting_floor
    }
    /// Projects the exact floor measured by this binding fate.
    #[must_use]
    pub const fn observer_progress_projection(&self) -> ObserverProgressProjection {
        ObserverProgressProjection::new(self.conversation_id, self.resulting_floor)
    }

    /// Selects direct `DetachedCursorRelease` when no storage edge precedes it.
    #[must_use]
    pub const fn into_direct_state(self, debt: ClosureDebt) -> ClosureState {
        owed(debt, StoredEdge::DetachedCursorRelease(self.release))
    }
}

/// Opaque proof that an exact marker-fenced attach committed.
///
/// Only the move-consuming frontier-owner mint constructs this value. Ordinary
/// attach therefore cannot fabricate marker acceptance or advance a cursor
/// merely by presenting a marker sequence.
#[derive(Debug, PartialEq, Eq)]
pub struct FencedAttachCommit {
    conversation_id: ConversationId,
    participant_id: ParticipantId,
    marker_delivery_seq: DeliverySeq,
    prior_binding_epoch: BindingEpoch,
    new_binding_epoch: BindingEpoch,
    next_state: ClosureState,
}

impl FencedAttachCommit {
    /// Returns the conversation inherited from the consumed recovery edge.
    #[must_use]
    pub const fn conversation_id(&self) -> ConversationId {
        self.conversation_id
    }

    /// Returns the participant whose fenced recovery committed.
    #[must_use]
    pub const fn participant_id(&self) -> ParticipantId {
        self.participant_id
    }

    /// Returns the exact delivered marker accepted by the commit.
    #[must_use]
    pub const fn marker_delivery_seq(&self) -> DeliverySeq {
        self.marker_delivery_seq
    }

    /// Returns the exact dead binding epoch that durably received the marker.
    #[must_use]
    pub const fn prior_binding_epoch(&self) -> BindingEpoch {
        self.prior_binding_epoch
    }

    /// Returns the exact newly committed authoritative binding epoch.
    #[must_use]
    pub const fn new_binding_epoch(&self) -> BindingEpoch {
        self.new_binding_epoch
    }

    /// Returns the measured clear, observer-projection, or compaction successor.
    #[must_use]
    pub const fn next_state(&self) -> ClosureState {
        self.next_state
    }

    /// Revalidates a durable fenced proof against its retained marker record.
    ///
    /// This is restoration, not a proof mint: the frontier continues to own the
    /// retained record while storage reconstructs the already-committed proof.
    pub(super) fn restore_validated(
        recovery: DetachedCredentialRecovery,
        record_authority: &ValidatedMarkerRecord,
        debt: ClosureDebt,
        event: Event,
        successor: DebtCompletion,
    ) -> Option<Self> {
        if debt.value().is_zero()
            || record_authority.conversation_id() != recovery.conversation_id
            || record_authority.participant_id() != recovery.participant_id
            || record_authority.delivery_seq() != recovery.marker_delivery_seq
            || record_authority.target_binding()
                != super::FrontierBinding::Detached(recovery.prior_binding_epoch)
            || record_authority.occurrence()
                != super::claim_frontier::MarkerRecordOccurrence::Delivered
        {
            return None;
        }
        let EventKind::FencedRecoveryCommitted {
            participant_id,
            marker_delivery_seq,
            prior_binding_epoch,
            new_binding_epoch,
            ..
        } = event.0
        else {
            return None;
        };
        if participant_id != recovery.participant_id
            || marker_delivery_seq != recovery.marker_delivery_seq
            || prior_binding_epoch != recovery.prior_binding_epoch
            || !is_next_generation(prior_binding_epoch, new_binding_epoch)
        {
            return None;
        }
        Some(Self {
            conversation_id: recovery.conversation_id,
            participant_id,
            marker_delivery_seq,
            prior_binding_epoch,
            new_binding_epoch,
            next_state: successor.into_state(),
        })
    }

    /// Validates the exact fate of this commit's recovered binding epoch.
    ///
    /// The returned authority retains both the fenced-attach provenance and its
    /// exact nonzero-debt OP/PC successor. It must be consumed by that stored
    /// edge's recovered-fate transition, so a fate that precedes storage
    /// completion cannot lose the required `DetachedCursorRelease` suffix.
    ///
    /// # Errors
    ///
    /// Returns the unchanged post-attach state unless the event names this
    /// participant and the exact newly committed binding epoch, or when the
    /// fenced attach had already cleared debt.
    pub(super) fn recovered_binding_fate(
        self,
        event: Event,
    ) -> Result<RecoveredBindingFate, Box<Self>> {
        let EventKind::BindingFateObserved {
            participant_id,
            binding_epoch,
            resulting_floor,
        } = event.0
        else {
            return Err(Box::new(self));
        };
        if participant_id != self.participant_id || binding_epoch != self.new_binding_epoch {
            return Err(Box::new(self));
        }
        let ClosureState::Owed { debt, edge } = self.next_state else {
            return Err(Box::new(self));
        };
        let predecessor = match edge {
            StoredEdge::ObserverProjection(value) => {
                RecoveredStorageEdge::ObserverProjection(value)
            }
            StoredEdge::PhysicalCompaction(value) => {
                RecoveredStorageEdge::PhysicalCompaction(value)
            }
            _ => return Err(Box::new(self)),
        };
        Ok(RecoveredBindingFate {
            conversation_id: self.conversation_id,
            predecessor_debt: debt,
            predecessor,
            resulting_floor,
            release: DetachedCursorRelease {
                participant_id,
                last_dead_binding_epoch: binding_epoch,
            },
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RecoveredStorageEdge {
    ObserverProjection(ObserverProjection),
    PhysicalCompaction(PhysicalCompaction),
}

impl RecoveredStorageEdge {
    const fn into_stored_edge(self) -> StoredEdge {
        match self {
            Self::ObserverProjection(value) => StoredEdge::ObserverProjection(value),
            Self::PhysicalCompaction(value) => StoredEdge::PhysicalCompaction(value),
        }
    }
}

/// Exact recovered-binding fate authority derived from a fenced attach.
///
/// Fields are private: only `FencedAttachCommit::recovered_binding_fate` can
/// bind a no-marker cursor release to the newly recovered epoch and the exact
/// OP/PC state installed by that attach.
#[derive(Debug, PartialEq, Eq)]
pub struct RecoveredBindingFate {
    conversation_id: ConversationId,
    predecessor_debt: ClosureDebt,
    predecessor: RecoveredStorageEdge,
    resulting_floor: DeliverySeq,
    release: DetachedCursorRelease,
}

impl RecoveredBindingFate {
    /// Returns the conversation inherited from the fenced-attach provenance.
    #[must_use]
    pub const fn conversation_id(&self) -> ConversationId {
        self.conversation_id
    }

    /// Returns the exact post-attach state to which this authority is bound.
    #[must_use]
    pub const fn predecessor_state(&self) -> ClosureState {
        owed(self.predecessor_debt, self.predecessor.into_stored_edge())
    }

    /// Returns the participant whose recovered binding died.
    #[must_use]
    pub const fn participant_id(&self) -> ParticipantId {
        self.release.participant_id
    }

    /// Returns the exact recovered epoch whose fate was observed.
    #[must_use]
    pub const fn last_dead_binding_epoch(&self) -> BindingEpoch {
        self.release.last_dead_binding_epoch
    }

    /// Returns the floor measured in the binding-fate transaction.
    #[must_use]
    pub const fn resulting_floor(&self) -> DeliverySeq {
        self.resulting_floor
    }
    /// Projects the exact floor measured by this recovered binding fate.
    #[must_use]
    pub const fn observer_progress_projection(&self) -> ObserverProgressProjection {
        ObserverProgressProjection::new(self.conversation_id, self.resulting_floor)
    }
}

/// Latent cursor-release suffix while an earlier OP/PC witness remains stored.
///
/// This opaque value must survive alongside the preserved storage edge. Exact
/// completion of that edge consumes it and installs `DetachedCursorRelease`, or
/// clears it only when closure debt reaches zero. Both ordinary binding fate
/// and fenced recovered fate produce this common post-provenance state.
#[derive(Debug, PartialEq, Eq)]
pub struct PendingRecoveredCursorRelease {
    debt: ClosureDebt,
    predecessor: RecoveredStorageEdge,
    release: DetachedCursorRelease,
}

impl PendingRecoveredCursorRelease {
    /// Returns the exact OP/PC state that remains current before completion.
    #[must_use]
    pub const fn current_state(&self) -> ClosureState {
        owed(self.debt, self.predecessor.into_stored_edge())
    }

    /// Returns the participant whose cursor release is pending.
    #[must_use]
    pub const fn participant_id(&self) -> ParticipantId {
        self.release.participant_id
    }

    /// Returns the exact recovered epoch whose cursor release is pending.
    #[must_use]
    pub const fn last_dead_binding_epoch(&self) -> BindingEpoch {
        self.release.last_dead_binding_epoch
    }
}

/// Exact released state when binding fate covers storage immediately.
#[derive(Debug, PartialEq, Eq)]
pub struct RecoveredCursorRelease {
    debt: ClosureDebt,
    release: DetachedCursorRelease,
}

impl RecoveredCursorRelease {
    /// Returns the nonzero debt carried by the cursor-release edge.
    #[must_use]
    pub const fn debt(&self) -> ClosureDebt {
        self.debt
    }

    /// Returns the exact derived cursor-release witness.
    #[must_use]
    pub const fn edge(&self) -> DetachedCursorRelease {
        self.release
    }

    /// Installs the exact derived cursor-release state.
    #[must_use]
    pub const fn into_state(self) -> ClosureState {
        owed(self.debt, StoredEdge::DetachedCursorRelease(self.release))
    }
}

/// Preserve-or-cover result for cursor-releasing binding fate against OP/PC.
#[derive(Debug, PartialEq, Eq)]
pub enum RecoveredBindingFateTransition {
    /// Storage remains current and carries a latent cursor-release suffix.
    PendingStorage(PendingRecoveredCursorRelease),
    /// The fate floor covered storage and selected cursor release immediately.
    DetachedCursorRelease(RecoveredCursorRelease),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SuccessorUse {
    ObserverCompletion,
    ObserverMarkerAppend,
    ObserverLeave,
    PhysicalCompletion,
    PhysicalCover,
    CursorGreaterAck,
}

/// Strict/later successor authority for OP, PC, and greater cumulative ack.
///
/// The predecessor, consumed event, and validated resulting state are private.
/// A value can be obtained only from the exact predecessor edge's builder, so a
/// caller cannot substitute an earlier edge or direct DCR at application time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProjectionCompactionSuccessor {
    predecessor: StoredEdge,
    event: Event,
    use_kind: SuccessorUse,
    state: ClosureState,
}

/// Closed cursor-fate result taxonomy retained for API compatibility.
///
/// [`ParticipantCursorProgress::binding_fate`] produces only the marker-backed
/// recovery arm. Executable cursor release is instead installed through
/// [`OrdinaryBindingFate`] or [`RecoveredBindingFate`], both of which carry the
/// required predecessor authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CursorFateSuccessor {
    /// Marker-backed state becomes fenced detached recovery.
    DetachedCredentialRecovery(DetachedCredentialRecovery),
    /// Reserved cursor-release taxonomy arm; raw continuous fate cannot produce it.
    DetachedCursorRelease(DetachedCursorRelease),
}

impl CursorFateSuccessor {
    /// Converts the derived fate into its stored edge.
    #[must_use]
    pub const fn into_stored_edge(self) -> StoredEdge {
        match self {
            Self::DetachedCredentialRecovery(value) => {
                StoredEdge::DetachedCredentialRecovery(value)
            }
            Self::DetachedCursorRelease(value) => StoredEdge::DetachedCursorRelease(value),
        }
    }
}

/// Exact refusal selected by a detached edge or charged retarget check.
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
    /// The proposed positive churn delta exceeds the episode limit.
    EpisodeChurnLimit,
    /// The proposed binding epoch does not immediately supersede this epoch.
    StaleAuthority,
    /// Binding-required work cannot run for a detached edge owner.
    NoBinding,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DetachedClaimTarget {
    CredentialRecovery {
        marker_delivery_seq: DeliverySeq,
        binding_epoch: BindingEpoch,
    },
    MarkerRelease {
        marker_delivery_seq: DeliverySeq,
        binding_epoch: BindingEpoch,
    },
    CursorRelease {
        binding_epoch: BindingEpoch,
    },
}

/// Validated evidence for an exact-current K-backed detached Leave.
///
/// There is no public constructor. Each detached edge validates participant,
/// exact edge target, positive actual record charge, remaining K, and the
/// available exit claim before producing this edge-bound value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KClaimBackedDetachedLeave {
    participant_id: ParticipantId,
    target: DetachedClaimTarget,
    actual_charge: ResourceVector,
}

impl KClaimBackedDetachedLeave {
    /// Returns the exact detached participant.
    #[must_use]
    pub const fn participant_id(self) -> ParticipantId {
        self.participant_id
    }

    /// Returns the exact charge already checked against remaining K.
    #[must_use]
    pub const fn actual_charge(self) -> ResourceVector {
        self.actual_charge
    }
}

/// Opaque typed completion event.
///
/// Constructors validate each event's local scalar shape. Edge transitions then
/// consume the event and match its participant, binding, marker, range, and
/// boundary against the exact stored predecessor. The private kind set is the
/// frozen eight-kind register: marker and normal acknowledgements share
/// `CursorProgressed`, while live and detached alternatives share
/// `LeaveCommitted`; the convenience constructors do not invent occurrences.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Event(EventKind);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EventKind {
    ProjectionCompleted {
        through_seq: DeliverySeq,
    },
    CompactionCompleted {
        from_floor: DeliverySeq,
        through_seq: DeliverySeq,
        resulting_floor: DeliverySeq,
    },
    MarkerAppended {
        marker_delivery_seq: DeliverySeq,
        resulting_projection_through: DeliverySeq,
    },
    MarkerDelivered {
        participant_id: ParticipantId,
        binding_epoch: BindingEpoch,
        marker_delivery_seq: DeliverySeq,
    },
    CursorProgressed {
        participant_index: ParticipantIndex,
        participant_id: ParticipantId,
        binding_epoch: BindingEpoch,
        progress: CursorProgressEvent,
        resulting_floor: DeliverySeq,
    },
    BindingFateObserved {
        participant_id: ParticipantId,
        binding_epoch: BindingEpoch,
        resulting_floor: DeliverySeq,
    },
    LeaveCommitted {
        participant_id: ParticipantId,
        authority: LeaveAuthority,
        resulting_floor: DeliverySeq,
    },
    FencedRecoveryCommitted {
        participant_id: ParticipantId,
        marker_delivery_seq: DeliverySeq,
        prior_binding_epoch: BindingEpoch,
        new_binding_epoch: BindingEpoch,
        resulting_floor: DeliverySeq,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CursorProgressEvent {
    Normal {
        previous_cursor: DeliverySeq,
        through_seq: DeliverySeq,
    },
    Marker {
        marker_delivery_seq: DeliverySeq,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LeaveAuthority {
    Live(BindingEpoch),
    Detached,
}

impl Event {
    /// Records observer projection through an exact sequence.
    #[must_use]
    pub const fn projection_completed(through_seq: DeliverySeq) -> Self {
        Self(EventKind::ProjectionCompleted { through_seq })
    }

    /// Records exact physical compaction and its resulting floor.
    #[must_use]
    pub const fn compaction_completed(
        from_floor: DeliverySeq,
        through_seq: DeliverySeq,
        resulting_floor: DeliverySeq,
    ) -> Option<Self> {
        if from_floor <= through_seq && resulting_floor > through_seq {
            Some(Self(EventKind::CompactionCompleted {
                from_floor,
                through_seq,
                resulting_floor,
            }))
        } else {
            None
        }
    }

    /// Records a preclaimed marker append that extends an OP suffix.
    #[must_use]
    pub const fn marker_appended(
        marker_delivery_seq: DeliverySeq,
        resulting_projection_through: DeliverySeq,
    ) -> Self {
        Self(EventKind::MarkerAppended {
            marker_delivery_seq,
            resulting_projection_through,
        })
    }

    /// Records final-emitter delivery of an exact marker to an exact epoch.
    #[must_use]
    pub const fn marker_delivered(
        participant_id: ParticipantId,
        binding_epoch: BindingEpoch,
        marker_delivery_seq: DeliverySeq,
    ) -> Self {
        Self(EventKind::MarkerDelivered {
            participant_id,
            binding_epoch,
            marker_delivery_seq,
        })
    }

    /// Records a strictly advancing cumulative normal ack.
    ///
    /// V1's permanent participant id is the participant index, so the stored
    /// occurrence key is derived from `participant_id` rather than accepted as
    /// an independently forgeable value.
    #[must_use]
    pub const fn cursor_progressed(
        participant_id: ParticipantId,
        binding_epoch: BindingEpoch,
        previous_cursor: DeliverySeq,
        through_seq: DeliverySeq,
        resulting_floor: DeliverySeq,
    ) -> Option<Self> {
        if through_seq > previous_cursor {
            Some(Self(EventKind::CursorProgressed {
                participant_index: participant_id,
                participant_id,
                binding_epoch,
                progress: CursorProgressEvent::Normal {
                    previous_cursor,
                    through_seq,
                },
                resulting_floor,
            }))
        } else {
            None
        }
    }

    /// Records acceptance of one exact delivered marker, deriving its
    /// participant-index occurrence key from the permanent participant id.
    #[must_use]
    pub const fn marker_acknowledged(
        participant_id: ParticipantId,
        binding_epoch: BindingEpoch,
        marker_delivery_seq: DeliverySeq,
        resulting_floor: DeliverySeq,
    ) -> Self {
        Self(EventKind::CursorProgressed {
            participant_index: participant_id,
            participant_id,
            binding_epoch,
            progress: CursorProgressEvent::Marker {
                marker_delivery_seq,
            },
            resulting_floor,
        })
    }

    /// Records exact binding fate and its measured floor effect.
    #[must_use]
    pub const fn binding_fate_observed(
        participant_id: ParticipantId,
        binding_epoch: BindingEpoch,
        resulting_floor: DeliverySeq,
    ) -> Self {
        Self(EventKind::BindingFateObserved {
            participant_id,
            binding_epoch,
            resulting_floor,
        })
    }

    /// Records a live-bound Leave and its measured floor effect.
    #[must_use]
    pub const fn live_leave_committed(
        participant_id: ParticipantId,
        binding_epoch: BindingEpoch,
        resulting_floor: DeliverySeq,
    ) -> Self {
        Self(EventKind::LeaveCommitted {
            participant_id,
            authority: LeaveAuthority::Live(binding_epoch),
            resulting_floor,
        })
    }

    /// Records a detached Leave and its measured floor effect.
    #[must_use]
    pub const fn detached_leave_committed(
        participant_id: ParticipantId,
        resulting_floor: DeliverySeq,
    ) -> Self {
        Self(EventKind::LeaveCommitted {
            participant_id,
            authority: LeaveAuthority::Detached,
            resulting_floor,
        })
    }

    /// Records exact fenced recovery into a new binding epoch.
    #[must_use]
    pub const fn fenced_recovery_committed(
        participant_id: ParticipantId,
        marker_delivery_seq: DeliverySeq,
        prior_binding_epoch: BindingEpoch,
        new_binding_epoch: BindingEpoch,
        resulting_floor: DeliverySeq,
    ) -> Self {
        Self(EventKind::FencedRecoveryCommitted {
            participant_id,
            marker_delivery_seq,
            prior_binding_epoch,
            new_binding_epoch,
            resulting_floor,
        })
    }
}

impl ObserverProjection {
    /// Applies ordinary no-marker binding fate while this OP remains current.
    ///
    /// The opaque fate carries ordinary-attach and exact-terminal provenance;
    /// projection completion must retain its cursor-release suffix while debt
    /// remains.
    #[must_use]
    #[allow(
        dead_code,
        reason = "the crate-owned binding-fate operation invokes this sealed OP transition"
    )]
    pub const fn apply_ordinary_binding_fate(
        self,
        resulting_debt: ClosureDebt,
        authority: OrdinaryBindingFate,
    ) -> PendingRecoveredCursorRelease {
        PendingRecoveredCursorRelease {
            debt: resulting_debt,
            predecessor: RecoveredStorageEdge::ObserverProjection(self),
            release: authority.release,
        }
    }

    /// Applies recovered binding fate while this exact OP remains incomplete.
    ///
    /// OP is independent of binding fate, so nonzero debt preserves it and the
    /// returned opaque value retains the exact `DetachedCursorRelease` suffix
    /// until projection completion.
    ///
    /// # Errors
    ///
    /// Returns the unconsumed authority unless it was derived for this exact OP
    /// and its exact post-attach debt.
    pub fn apply_recovered_binding_fate(
        self,
        debt: ClosureDebt,
        resulting_debt: ClosureDebt,
        authority: RecoveredBindingFate,
    ) -> Result<RecoveredBindingFateTransition, RecoveredBindingFate> {
        if authority.predecessor != RecoveredStorageEdge::ObserverProjection(self)
            || authority.predecessor_debt != debt
        {
            return Err(authority);
        }
        Ok(RecoveredBindingFateTransition::PendingStorage(
            PendingRecoveredCursorRelease {
                debt: resulting_debt,
                predecessor: RecoveredStorageEdge::ObserverProjection(self),
                release: authority.release,
            },
        ))
    }

    /// Consumes a latent recovered cursor suffix on exact OP completion.
    ///
    /// # Errors
    ///
    /// Returns the pending authority intact unless it belongs to this exact OP
    /// and the completion event reaches its exact boundary.
    pub fn complete_after_recovered_binding_fate(
        self,
        event: Event,
        resulting_debt: Option<ClosureDebt>,
        pending: PendingRecoveredCursorRelease,
    ) -> Result<ClosureState, PendingRecoveredCursorRelease> {
        self.complete_after_binding_fate(event, resulting_debt, pending)
    }

    /// Consumes a latent ordinary cursor suffix on exact OP completion.
    ///
    /// # Errors
    ///
    /// Returns the pending authority intact unless it belongs to this exact OP
    /// and the completion event reaches the stored projection boundary.
    pub fn complete_after_ordinary_binding_fate(
        self,
        event: Event,
        resulting_debt: Option<ClosureDebt>,
        pending: PendingRecoveredCursorRelease,
    ) -> Result<ClosureState, PendingRecoveredCursorRelease> {
        self.complete_after_binding_fate(event, resulting_debt, pending)
    }

    /// Consumes a latent cursor-release suffix on exact OP completion.
    ///
    /// # Errors
    ///
    /// Returns the pending authority intact unless it belongs to this exact OP
    /// and the completion event reaches its exact boundary.
    pub(crate) fn complete_after_binding_fate(
        self,
        event: Event,
        resulting_debt: Option<ClosureDebt>,
        pending: PendingRecoveredCursorRelease,
    ) -> Result<ClosureState, PendingRecoveredCursorRelease> {
        if pending.predecessor != RecoveredStorageEdge::ObserverProjection(self)
            || projection_completion_boundary(self, event).is_none()
        {
            return Err(pending);
        }
        Ok(preserve_or_clear(
            resulting_debt,
            StoredEdge::DetachedCursorRelease(pending.release),
        ))
    }

    /// Validates clear selection after this exact projection completes.
    #[must_use]
    pub const fn clear_after_completion(
        &self,
        event: &Event,
    ) -> Option<ProjectionCompactionSuccessor> {
        if projection_completion_boundary(*self, *event).is_some() {
            Some(ProjectionCompactionSuccessor {
                predecessor: StoredEdge::ObserverProjection(*self),
                event: *event,
                use_kind: SuccessorUse::ObserverCompletion,
                state: ClosureState::Clear,
            })
        } else {
            None
        }
    }

    /// Validates a non-DCR strict suffix after this exact projection completes.
    #[must_use]
    pub const fn strict_after_completion(
        &self,
        event: &Event,
        debt: ClosureDebt,
        edge: StoredEdge,
        successor_boundary: DeliverySeq,
    ) -> Option<ProjectionCompactionSuccessor> {
        let Some(completed_through) = projection_completion_boundary(*self, *event) else {
            return None;
        };
        if successor_boundary <= completed_through
            || !strict_edge_matches_boundary(edge, successor_boundary)
        {
            return None;
        }
        Some(ProjectionCompactionSuccessor {
            predecessor: StoredEdge::ObserverProjection(*self),
            event: *event,
            use_kind: SuccessorUse::ObserverCompletion,
            state: ClosureState::Owed { debt, edge },
        })
    }

    /// Consumes exact projection completion and its predecessor-bound successor.
    ///
    /// # Errors
    ///
    /// Returns the unchanged owed state when the event or successor authority
    /// was not built for this exact projection.
    pub fn complete(
        self,
        debt: ClosureDebt,
        event: Event,
        successor: ProjectionCompactionSuccessor,
    ) -> Result<ClosureState, ClosureState> {
        let original = owed(debt, StoredEdge::ObserverProjection(self));
        if successor.predecessor == StoredEdge::ObserverProjection(self)
            && successor.event == event
            && successor.use_kind == SuccessorUse::ObserverCompletion
            && projection_completion_boundary(self, event).is_some()
        {
            Ok(successor.state)
        } else {
            Err(original)
        }
    }

    /// Validates the exact later OP selected by a preclaimed marker append.
    #[must_use]
    pub const fn later_projection_after_marker(
        &self,
        event: &Event,
        debt: ClosureDebt,
        successor: Self,
    ) -> Option<ProjectionCompactionSuccessor> {
        let EventKind::MarkerAppended {
            marker_delivery_seq,
            resulting_projection_through,
        } = event.0
        else {
            return None;
        };
        if marker_delivery_seq <= self.through_seq
            || resulting_projection_through < marker_delivery_seq
            || successor.through_seq != resulting_projection_through
        {
            return None;
        }
        Some(ProjectionCompactionSuccessor {
            predecessor: StoredEdge::ObserverProjection(*self),
            event: *event,
            use_kind: SuccessorUse::ObserverMarkerAppend,
            state: owed(debt, StoredEdge::ObserverProjection(successor)),
        })
    }

    /// Consumes the marker occurrence and atomically installs its exact later OP.
    ///
    /// # Errors
    ///
    /// Returns the unchanged owed state when the event-bound successor was
    /// built for another projection or occurrence.
    pub fn marker_appended(
        self,
        debt: ClosureDebt,
        event: Event,
        successor: ProjectionCompactionSuccessor,
    ) -> Result<ClosureState, ClosureState> {
        let original = owed(debt, StoredEdge::ObserverProjection(self));
        if successor.predecessor == StoredEdge::ObserverProjection(self)
            && successor.event == event
            && successor.use_kind == SuccessorUse::ObserverMarkerAppend
        {
            Ok(successor.state)
        } else {
            Err(original)
        }
    }

    /// Validates the exact later OP selected atomically by a live or detached Leave.
    #[must_use]
    pub const fn later_projection_after_leave(
        &self,
        event: &Event,
        debt: ClosureDebt,
        successor: Self,
    ) -> Option<ProjectionCompactionSuccessor> {
        let EventKind::LeaveCommitted {
            resulting_floor, ..
        } = event.0
        else {
            return None;
        };
        if successor.through_seq <= self.through_seq || successor.through_seq < resulting_floor {
            return None;
        }
        Some(ProjectionCompactionSuccessor {
            predecessor: StoredEdge::ObserverProjection(*self),
            event: *event,
            use_kind: SuccessorUse::ObserverLeave,
            state: owed(debt, StoredEdge::ObserverProjection(successor)),
        })
    }

    /// Consumes exact Leave and atomically installs its predecessor-bound later OP.
    ///
    /// # Errors
    ///
    /// Returns the unchanged owed state unless the successor was built for
    /// this exact OP and Leave occurrence.
    pub fn leave_with_later_projection(
        self,
        debt: ClosureDebt,
        event: Event,
        successor: ProjectionCompactionSuccessor,
    ) -> Result<ClosureState, ClosureState> {
        let original = owed(debt, StoredEdge::ObserverProjection(self));
        if successor.predecessor == StoredEdge::ObserverProjection(self)
            && successor.event == event
            && successor.use_kind == SuccessorUse::ObserverLeave
        {
            Ok(successor.state)
        } else {
            Err(original)
        }
    }

    /// Consumes an independently valid cursor, marker, fate, or Leave event and
    /// preserves this exact OP while debt remains, or clears it with debt.
    ///
    /// # Errors
    ///
    /// Returns the unchanged owed state for an event outside those independent
    /// invalidator classes.
    pub const fn independent_event(
        self,
        debt: ClosureDebt,
        event: Event,
        resulting_debt: Option<ClosureDebt>,
    ) -> Result<ClosureState, ClosureState> {
        let original = owed(debt, StoredEdge::ObserverProjection(self));
        if !matches!(
            event.0,
            EventKind::CursorProgressed { .. }
                | EventKind::BindingFateObserved { .. }
                | EventKind::LeaveCommitted { .. }
        ) {
            return Err(original);
        }
        Ok(preserve_or_clear(
            resulting_debt,
            StoredEdge::ObserverProjection(self),
        ))
    }

    /// Applies a binding change only after a positive charged churn preflight.
    ///
    /// # Errors
    ///
    /// Returns the unchanged state and `EpisodeChurnLimit` when the delta is
    /// zero or would exceed the episode limit.
    pub const fn charged_binding_change(
        self,
        debt: ClosureDebt,
        episode_churn_used: u64,
        delta_cycles: u64,
        episode_churn_limit: u64,
        resulting_debt: Option<ClosureDebt>,
    ) -> Result<ClosureState, (ClosureState, DetachedAttachRefusal)> {
        let original = owed(debt, StoredEdge::ObserverProjection(self));
        if !charged_churn_fits(episode_churn_used, delta_cycles, episode_churn_limit) {
            return Err((original, DetachedAttachRefusal::EpisodeChurnLimit));
        }
        Ok(preserve_or_clear(
            resulting_debt,
            StoredEdge::ObserverProjection(self),
        ))
    }
}

impl PhysicalCompaction {
    /// Applies ordinary no-marker binding fate by preserving or covering PC.
    #[must_use]
    #[allow(
        dead_code,
        reason = "the crate-owned binding-fate replay boundary invokes this sealed PC transition"
    )]
    pub(crate) const fn apply_ordinary_binding_fate(
        self,
        resulting_debt: ClosureDebt,
        authority: OrdinaryBindingFate,
    ) -> RecoveredBindingFateTransition {
        if authority.resulting_floor > self.through_seq {
            RecoveredBindingFateTransition::DetachedCursorRelease(RecoveredCursorRelease {
                debt: resulting_debt,
                release: authority.release,
            })
        } else {
            RecoveredBindingFateTransition::PendingStorage(PendingRecoveredCursorRelease {
                debt: resulting_debt,
                predecessor: RecoveredStorageEdge::PhysicalCompaction(self),
                release: authority.release,
            })
        }
    }

    /// Applies recovered binding fate by preserving or covering this exact PC.
    ///
    /// A fate floor at or below `through_seq` preserves PC and returns a latent
    /// cursor-release suffix. A greater floor covers PC immediately and selects
    /// the exact cursor release derived from the fenced attach.
    ///
    /// # Errors
    ///
    /// Returns the unconsumed authority unless it was derived for this exact PC
    /// and its exact post-attach debt.
    pub fn apply_recovered_binding_fate(
        self,
        debt: ClosureDebt,
        resulting_debt: ClosureDebt,
        authority: RecoveredBindingFate,
    ) -> Result<RecoveredBindingFateTransition, RecoveredBindingFate> {
        if authority.predecessor != RecoveredStorageEdge::PhysicalCompaction(self)
            || authority.predecessor_debt != debt
        {
            return Err(authority);
        }
        if authority.resulting_floor > self.through_seq {
            Ok(RecoveredBindingFateTransition::DetachedCursorRelease(
                RecoveredCursorRelease {
                    debt: resulting_debt,
                    release: authority.release,
                },
            ))
        } else {
            Ok(RecoveredBindingFateTransition::PendingStorage(
                PendingRecoveredCursorRelease {
                    debt: resulting_debt,
                    predecessor: RecoveredStorageEdge::PhysicalCompaction(self),
                    release: authority.release,
                },
            ))
        }
    }

    /// Consumes a latent recovered cursor suffix on exact PC completion.
    ///
    /// # Errors
    ///
    /// Returns the pending authority intact unless it belongs to this exact PC
    /// and the completion event covers its exact stored range.
    pub fn complete_after_recovered_binding_fate(
        self,
        event: Event,
        resulting_debt: Option<ClosureDebt>,
        pending: PendingRecoveredCursorRelease,
    ) -> Result<ClosureState, PendingRecoveredCursorRelease> {
        self.complete_after_binding_fate(event, resulting_debt, pending)
    }

    /// Consumes a latent cursor-release suffix on exact PC completion.
    ///
    /// # Errors
    ///
    /// Returns the pending authority intact unless it belongs to this exact PC
    /// and the completion event covers its exact stored range.
    pub(crate) fn complete_after_binding_fate(
        self,
        event: Event,
        resulting_debt: Option<ClosureDebt>,
        pending: PendingRecoveredCursorRelease,
    ) -> Result<ClosureState, PendingRecoveredCursorRelease> {
        if pending.predecessor != RecoveredStorageEdge::PhysicalCompaction(self)
            || physical_completion_floor(self, event).is_none()
        {
            return Err(pending);
        }
        Ok(preserve_or_clear(
            resulting_debt,
            StoredEdge::DetachedCursorRelease(pending.release),
        ))
    }

    /// Validates clear selection after exact PC completion.
    #[must_use]
    pub const fn clear_after_completion(
        &self,
        event: &Event,
    ) -> Option<ProjectionCompactionSuccessor> {
        if physical_completion_floor(*self, *event).is_some() {
            Some(ProjectionCompactionSuccessor {
                predecessor: StoredEdge::PhysicalCompaction(*self),
                event: *event,
                use_kind: SuccessorUse::PhysicalCompletion,
                state: ClosureState::Clear,
            })
        } else {
            None
        }
    }

    /// Validates a non-DCR strict suffix after exact PC completion.
    #[must_use]
    pub const fn strict_after_completion(
        &self,
        event: &Event,
        debt: ClosureDebt,
        edge: StoredEdge,
        successor_boundary: DeliverySeq,
    ) -> Option<ProjectionCompactionSuccessor> {
        let Some(resulting_floor) = physical_completion_floor(*self, *event) else {
            return None;
        };
        if successor_boundary < resulting_floor
            || !strict_edge_matches_boundary(edge, successor_boundary)
        {
            return None;
        }
        Some(ProjectionCompactionSuccessor {
            predecessor: StoredEdge::PhysicalCompaction(*self),
            event: *event,
            use_kind: SuccessorUse::PhysicalCompletion,
            state: ClosureState::Owed { debt, edge },
        })
    }

    /// Consumes exact PC completion.
    ///
    /// # Errors
    ///
    /// Returns the unchanged owed state when the event or successor authority
    /// does not belong to this exact compaction range.
    pub fn complete(
        self,
        debt: ClosureDebt,
        event: Event,
        successor: ProjectionCompactionSuccessor,
    ) -> Result<ClosureState, ClosureState> {
        let original = owed(debt, StoredEdge::PhysicalCompaction(self));
        if successor.predecessor == StoredEdge::PhysicalCompaction(self)
            && successor.event == event
            && successor.use_kind == SuccessorUse::PhysicalCompletion
            && physical_completion_floor(self, event).is_some()
        {
            Ok(successor.state)
        } else {
            Err(original)
        }
    }

    /// Records a later marker append while preserving this exact active range.
    ///
    /// # Errors
    ///
    /// Returns the unchanged owed state unless the appended marker lies
    /// strictly after the physical-compaction range and its projection target
    /// covers that marker.
    pub const fn marker_appended(
        self,
        debt: ClosureDebt,
        event: Event,
    ) -> Result<ClosureState, ClosureState> {
        let original = owed(debt, StoredEdge::PhysicalCompaction(self));
        let EventKind::MarkerAppended {
            marker_delivery_seq,
            resulting_projection_through,
        } = event.0
        else {
            return Err(original);
        };
        if marker_delivery_seq <= self.through_seq
            || resulting_projection_through < marker_delivery_seq
        {
            return Err(original);
        }
        Ok(original)
    }

    /// Applies an advancing ack, fate, or Leave whose resulting floor does not
    /// cover this PC, preserving the exact range while debt remains.
    ///
    /// # Errors
    ///
    /// Returns the unchanged owed state when the event is not a progress class
    /// or its resulting floor covers the stored range.
    pub const fn preserve_progress(
        self,
        debt: ClosureDebt,
        event: Event,
        resulting_debt: ClosureDebt,
    ) -> Result<ClosureState, ClosureState> {
        let original = owed(debt, StoredEdge::PhysicalCompaction(self));
        let Some(resulting_floor) = progress_event_floor(event) else {
            return Err(original);
        };
        if resulting_floor > self.through_seq {
            return Err(original);
        }
        Ok(owed(resulting_debt, StoredEdge::PhysicalCompaction(self)))
    }

    /// Validates clear selection when an ack, fate, or Leave covers this PC.
    #[must_use]
    pub const fn clear_after_progress(
        &self,
        event: &Event,
    ) -> Option<ProjectionCompactionSuccessor> {
        let Some(resulting_floor) = progress_event_floor(*event) else {
            return None;
        };
        if resulting_floor <= self.through_seq {
            return None;
        }
        Some(ProjectionCompactionSuccessor {
            predecessor: StoredEdge::PhysicalCompaction(*self),
            event: *event,
            use_kind: SuccessorUse::PhysicalCover,
            state: ClosureState::Clear,
        })
    }

    /// Validates a strict non-DCR suffix when an ack, fate, or Leave covers PC.
    #[must_use]
    pub const fn strict_after_progress(
        &self,
        event: &Event,
        debt: ClosureDebt,
        edge: StoredEdge,
        successor_boundary: DeliverySeq,
    ) -> Option<ProjectionCompactionSuccessor> {
        let Some(resulting_floor) = progress_event_floor(*event) else {
            return None;
        };
        if resulting_floor <= self.through_seq
            || successor_boundary < resulting_floor
            || !strict_edge_matches_boundary(edge, successor_boundary)
        {
            return None;
        }
        Some(ProjectionCompactionSuccessor {
            predecessor: StoredEdge::PhysicalCompaction(*self),
            event: *event,
            use_kind: SuccessorUse::PhysicalCover,
            state: ClosureState::Owed { debt, edge },
        })
    }

    /// Consumes the covering ack/fate/Leave event and its validated suffix.
    ///
    /// # Errors
    ///
    /// Returns the unchanged owed state when the successor authority is not
    /// bound to this range and event.
    pub fn covered_by_progress(
        self,
        debt: ClosureDebt,
        event: Event,
        successor: ProjectionCompactionSuccessor,
    ) -> Result<ClosureState, ClosureState> {
        let original = owed(debt, StoredEdge::PhysicalCompaction(self));
        if successor.predecessor == StoredEdge::PhysicalCompaction(self)
            && successor.event == event
            && successor.use_kind == SuccessorUse::PhysicalCover
        {
            Ok(successor.state)
        } else {
            Err(original)
        }
    }

    /// No-op and refused acknowledgements consume no event and preserve exact PC.
    #[must_use]
    pub const fn unchanged(self, debt: ClosureDebt) -> ClosureState {
        owed(debt, StoredEdge::PhysicalCompaction(self))
    }

    /// Applies a charged binding change that leaves this PC range uncovered.
    ///
    /// # Errors
    ///
    /// Returns the unchanged state with the precise churn or stale-selection
    /// refusal when charging fails or the measured floor covers the range.
    pub const fn charged_binding_change_preserving(
        self,
        debt: ClosureDebt,
        episode_churn_used: u64,
        delta_cycles: u64,
        episode_churn_limit: u64,
        resulting_floor: DeliverySeq,
        resulting_debt: ClosureDebt,
    ) -> Result<ClosureState, (ClosureState, DetachedAttachRefusal)> {
        let original = owed(debt, StoredEdge::PhysicalCompaction(self));
        if !charged_churn_fits(episode_churn_used, delta_cycles, episode_churn_limit) {
            return Err((original, DetachedAttachRefusal::EpisodeChurnLimit));
        }
        if resulting_floor > self.through_seq {
            return Err((original, DetachedAttachRefusal::StaleAuthority));
        }
        Ok(owed(resulting_debt, StoredEdge::PhysicalCompaction(self)))
    }

    /// Applies a charged binding change whose measured floor covers this PC.
    ///
    /// # Errors
    ///
    /// Returns the unchanged state with the precise churn or stale-selection
    /// refusal when charging fails or the proposed strict suffix is invalid.
    #[allow(clippy::too_many_arguments)]
    pub const fn charged_binding_change_covering(
        self,
        debt: ClosureDebt,
        episode_churn_used: u64,
        delta_cycles: u64,
        episode_churn_limit: u64,
        resulting_floor: DeliverySeq,
        resulting_debt: ClosureDebt,
        edge: StoredEdge,
        successor_boundary: DeliverySeq,
    ) -> Result<ClosureState, (ClosureState, DetachedAttachRefusal)> {
        let original = owed(debt, StoredEdge::PhysicalCompaction(self));
        if !charged_churn_fits(episode_churn_used, delta_cycles, episode_churn_limit) {
            return Err((original, DetachedAttachRefusal::EpisodeChurnLimit));
        }
        if resulting_floor <= self.through_seq
            || successor_boundary < resulting_floor
            || !strict_edge_matches_boundary(edge, successor_boundary)
        {
            return Err((original, DetachedAttachRefusal::StaleAuthority));
        }
        Ok(owed(resulting_debt, edge))
    }
}

impl MarkerDelivery {
    /// Consumes sealed marker-delivery authority and derives its exact cursor
    /// progress witness after validating the delivered event.
    ///
    /// This debt-independent projection is for owners that persist the marker
    /// successor separately from later closure-accounting evolution. Callers
    /// cannot mint `MarkerDelivery`; only a validated marker drain or restore
    /// can supply this authority.
    ///
    /// # Errors
    ///
    /// Returns the unchanged sealed delivery unless participant, epoch, and
    /// marker sequence exactly match.
    pub fn delivered_progress(self, event: Event) -> Result<ParticipantCursorProgress, Self> {
        let EventKind::MarkerDelivered {
            participant_id,
            binding_epoch,
            marker_delivery_seq,
        } = event.0
        else {
            return Err(self);
        };
        if participant_id != self.participant_id
            || binding_epoch != self.binding_epoch
            || marker_delivery_seq != self.marker_delivery_seq
        {
            return Err(self);
        }
        Ok(ParticipantCursorProgress::Marker(CursorProgressMarker {
            conversation_id: self.conversation_id,
            participant_id,
            binding_epoch,
            through_seq: marker_delivery_seq,
            marker_delivery_seq,
        }))
    }

    /// Consumes exact final-emitter delivery and derives marker-backed PCP.
    ///
    /// The PCP boundary is the delivered marker itself; callers cannot supply a
    /// different cursor witness.
    ///
    /// # Errors
    ///
    /// Returns the unchanged owed state unless participant, epoch, and marker
    /// exactly match this delivery witness.
    pub fn delivered(self, debt: ClosureDebt, event: Event) -> Result<ClosureState, ClosureState> {
        let original = owed(debt, StoredEdge::MarkerDelivery(self));
        let Ok(progress) = self.delivered_progress(event) else {
            return Err(original);
        };
        Ok(owed(debt, StoredEdge::ParticipantCursorProgress(progress)))
    }

    /// Applies a lower normal ack, projection, or compaction below the anchor,
    /// preserving exact delivery while debt remains or clearing it with debt.
    ///
    /// # Errors
    ///
    /// Returns the unchanged owed state when the event is not a permitted lower
    /// progress event or reaches the marker anchor.
    pub const fn lower_progress(
        self,
        debt: ClosureDebt,
        event: Event,
        resulting_debt: Option<ClosureDebt>,
    ) -> Result<ClosureState, ClosureState> {
        let original = owed(debt, StoredEdge::MarkerDelivery(self));
        let is_lower = match event.0 {
            EventKind::CursorProgressed {
                progress: CursorProgressEvent::Normal { through_seq, .. },
                ..
            }
            | EventKind::ProjectionCompleted { through_seq } => {
                through_seq < self.marker_delivery_seq
            }
            EventKind::CompactionCompleted {
                through_seq,
                resulting_floor,
                ..
            } => {
                through_seq < self.marker_delivery_seq
                    && resulting_floor <= self.marker_delivery_seq
            }
            _ => false,
        };
        if !is_lower {
            return Err(original);
        }
        Ok(preserve_or_clear(
            resulting_debt,
            StoredEdge::MarkerDelivery(self),
        ))
    }

    /// Consumes exact pre-delivery binding fate and derives Leave-only DMR.
    ///
    /// # Errors
    ///
    /// Returns the unchanged owed state unless fate names the exact participant
    /// and binding epoch targeted by this undelivered marker.
    pub fn binding_fate(
        self,
        debt: ClosureDebt,
        event: Event,
    ) -> Result<ClosureState, ClosureState> {
        let original = owed(debt, StoredEdge::MarkerDelivery(self));
        let EventKind::BindingFateObserved {
            participant_id,
            binding_epoch,
            ..
        } = event.0
        else {
            return Err(original);
        };
        if participant_id != self.participant_id || binding_epoch != self.binding_epoch {
            return Err(original);
        }
        Ok(owed(
            debt,
            StoredEdge::DetachedMarkerRelease(DetachedMarkerRelease {
                participant_id,
                marker_delivery_seq: self.marker_delivery_seq,
                last_dead_binding_epoch: binding_epoch,
            }),
        ))
    }

    /// Retargets undelivered marker delivery after exact charged supersession.
    ///
    /// # Errors
    ///
    /// Returns the unchanged delivery with `EpisodeChurnLimit` or
    /// `StaleAuthority` when charged churn or the next-generation check fails.
    pub const fn retarget(
        self,
        new_binding_epoch: BindingEpoch,
        episode_churn_used: u64,
        delta_cycles: u64,
        episode_churn_limit: u64,
    ) -> Result<Self, (Self, DetachedAttachRefusal)> {
        if !charged_churn_fits(episode_churn_used, delta_cycles, episode_churn_limit) {
            return Err((self, DetachedAttachRefusal::EpisodeChurnLimit));
        }
        if !is_next_generation(self.binding_epoch, new_binding_epoch) {
            return Err((self, DetachedAttachRefusal::StaleAuthority));
        }
        Ok(Self {
            binding_epoch: new_binding_epoch,
            ..self
        })
    }

    /// Consumes exact live Leave and installs only clear/OP/PC.
    ///
    /// # Errors
    ///
    /// Returns the unchanged owed state unless Leave names the exact live
    /// participant and binding epoch.
    pub fn leave(
        self,
        debt: ClosureDebt,
        event: Event,
        successor: DebtCompletion,
    ) -> Result<ClosureState, ClosureState> {
        let original = owed(debt, StoredEdge::MarkerDelivery(self));
        let EventKind::LeaveCommitted {
            participant_id,
            authority: LeaveAuthority::Live(binding_epoch),
            ..
        } = event.0
        else {
            return Err(original);
        };
        if participant_id != self.participant_id || binding_epoch != self.binding_epoch {
            return Err(original);
        }
        Ok(successor.into_state())
    }
}

impl ParticipantCursorProgress {
    /// Consumes an equal normal ack or exact marker ack and selects only
    /// clear/OP/PC.
    ///
    /// # Errors
    ///
    /// Returns the unchanged owed state unless the ack kind, participant,
    /// epoch, and boundary exactly satisfy this cursor witness.
    pub fn complete_ack(
        self,
        debt: ClosureDebt,
        event: Event,
        successor: DebtCompletion,
    ) -> Result<ClosureState, ClosureState> {
        let original = owed(debt, StoredEdge::ParticipantCursorProgress(self));
        let exact = match (self, event.0) {
            (
                Self::Continuous(value),
                EventKind::CursorProgressed {
                    participant_id,
                    binding_epoch,
                    progress: CursorProgressEvent::Normal { through_seq, .. },
                    ..
                },
            ) => {
                participant_id == value.participant_id
                    && binding_epoch == value.binding_epoch
                    && through_seq == value.through_seq
            }
            (
                Self::Marker(value),
                EventKind::CursorProgressed {
                    participant_id,
                    binding_epoch,
                    progress: CursorProgressEvent::Normal { through_seq, .. },
                    ..
                },
            ) => {
                participant_id == value.participant_id
                    && binding_epoch == value.binding_epoch
                    && through_seq == value.through_seq
            }
            (
                Self::Marker(value),
                EventKind::CursorProgressed {
                    participant_id,
                    binding_epoch,
                    progress:
                        CursorProgressEvent::Marker {
                            marker_delivery_seq,
                        },
                    ..
                },
            ) => {
                participant_id == value.participant_id
                    && binding_epoch == value.binding_epoch
                    && marker_delivery_seq == value.marker_delivery_seq
            }
            _ => false,
        };
        if exact {
            Ok(successor.into_state())
        } else {
            Err(original)
        }
    }

    /// Consumes a lesser advancing normal ack and preserves this exact PCP.
    ///
    /// # Errors
    ///
    /// Returns the unchanged owed state unless the event is a matching current-
    /// epoch normal ack strictly below the stored boundary.
    pub fn lesser_ack(
        self,
        debt: ClosureDebt,
        event: Event,
        resulting_debt: ClosureDebt,
    ) -> Result<ClosureState, ClosureState> {
        let original = owed(debt, StoredEdge::ParticipantCursorProgress(self));
        let EventKind::CursorProgressed {
            participant_id,
            binding_epoch,
            progress: CursorProgressEvent::Normal { through_seq, .. },
            ..
        } = event.0
        else {
            return Err(original);
        };
        if participant_id != self.participant_id()
            || binding_epoch != self.binding_epoch()
            || through_seq >= self.through_seq()
        {
            return Err(original);
        }
        Ok(owed(
            resulting_debt,
            StoredEdge::ParticipantCursorProgress(self),
        ))
    }

    /// Validates clear selection for one greater cumulative normal ack.
    #[must_use]
    pub fn clear_after_greater_ack(&self, event: &Event) -> Option<ProjectionCompactionSuccessor> {
        if !greater_ack_matches(*self, *event) {
            return None;
        }
        Some(ProjectionCompactionSuccessor {
            predecessor: StoredEdge::ParticipantCursorProgress(*self),
            event: *event,
            use_kind: SuccessorUse::CursorGreaterAck,
            state: ClosureState::Clear,
        })
    }

    /// Validates a strict non-DCR suffix for one greater cumulative normal ack.
    #[must_use]
    pub fn strict_after_greater_ack(
        &self,
        event: &Event,
        debt: ClosureDebt,
        edge: StoredEdge,
        successor_boundary: DeliverySeq,
    ) -> Option<ProjectionCompactionSuccessor> {
        if !greater_ack_matches(*self, *event)
            || successor_boundary <= cursor_event_boundary(*event)
            || !strict_edge_matches_boundary(edge, successor_boundary)
        {
            return None;
        }
        Some(ProjectionCompactionSuccessor {
            predecessor: StoredEdge::ParticipantCursorProgress(*self),
            event: *event,
            use_kind: SuccessorUse::CursorGreaterAck,
            state: ClosureState::Owed { debt, edge },
        })
    }

    /// Consumes a greater cumulative normal ack and its predecessor-bound suffix.
    ///
    /// # Errors
    ///
    /// Returns the unchanged owed state unless the advancing event and strict
    /// successor authority are both bound to this cursor witness.
    pub fn greater_ack(
        self,
        debt: ClosureDebt,
        event: Event,
        successor: ProjectionCompactionSuccessor,
    ) -> Result<ClosureState, ClosureState> {
        let original = owed(debt, StoredEdge::ParticipantCursorProgress(self));
        if successor.predecessor == StoredEdge::ParticipantCursorProgress(self)
            && successor.event == event
            && successor.use_kind == SuccessorUse::CursorGreaterAck
            && greater_ack_matches(self, event)
        {
            Ok(successor.state)
        } else {
            Err(original)
        }
    }

    /// No-op, `AckGap`, and `AckRegression` consume no event and preserve exact PCP.
    #[must_use]
    pub const fn unchanged(self, debt: ClosureDebt) -> ClosureState {
        owed(debt, StoredEdge::ParticipantCursorProgress(self))
    }

    /// Consumes independent projection/compaction completion and preserves PCP
    /// while debt remains, or clears it. Compaction cannot cross an unaccepted
    /// marker anchor.
    ///
    /// # Errors
    ///
    /// Returns the unchanged owed state for another event class or for a
    /// compaction that reaches an unaccepted marker.
    pub const fn storage_progress(
        self,
        debt: ClosureDebt,
        event: Event,
        resulting_debt: Option<ClosureDebt>,
    ) -> Result<ClosureState, ClosureState> {
        let original = owed(debt, StoredEdge::ParticipantCursorProgress(self));
        let valid = match event.0 {
            EventKind::ProjectionCompleted { .. } => true,
            EventKind::CompactionCompleted {
                through_seq,
                resulting_floor,
                ..
            } => match self.marker_delivery_seq() {
                None => true,
                Some(marker) => through_seq < marker && resulting_floor <= marker,
            },
            _ => false,
        };
        if !valid {
            return Err(original);
        }
        Ok(preserve_or_clear(
            resulting_debt,
            StoredEdge::ParticipantCursorProgress(self),
        ))
    }

    /// Consumes exact binding fate and derives DCR only from marker-backed PCP.
    ///
    /// Continuous PCP never accepts this raw-event transition. Ordinary
    /// no-marker fate requires [`OrdinaryBindingAuthority`], while the fate of
    /// an epoch committed by fenced attach requires
    /// `FencedAttachCommit::recovered_binding_fate`.
    ///
    /// # Errors
    ///
    /// Returns the unchanged owed state unless fate names the exact participant
    /// and binding epoch carried by this cursor witness.
    pub fn binding_fate(
        self,
        debt: ClosureDebt,
        event: Event,
    ) -> Result<CursorFateSuccessor, ClosureState> {
        let original = owed(debt, StoredEdge::ParticipantCursorProgress(self));
        let Self::Marker(value) = self else {
            return Err(original);
        };
        let EventKind::BindingFateObserved {
            participant_id,
            binding_epoch,
            ..
        } = event.0
        else {
            return Err(original);
        };
        if participant_id != value.participant_id || binding_epoch != value.binding_epoch {
            return Err(original);
        }
        Ok(CursorFateSuccessor::DetachedCredentialRecovery(
            DetachedCredentialRecovery {
                conversation_id: value.conversation_id,
                participant_id: value.participant_id,
                marker_delivery_seq: value.marker_delivery_seq,
                prior_binding_epoch: value.binding_epoch,
            },
        ))
    }

    /// Retargets only continuous PCP after exact charged supersession.
    ///
    /// # Errors
    ///
    /// Returns the unchanged cursor and the exact delivered-marker, churn, or
    /// stale-authority refusal when retargeting is forbidden.
    pub const fn retarget(
        self,
        new_binding_epoch: BindingEpoch,
        episode_churn_used: u64,
        delta_cycles: u64,
        episode_churn_limit: u64,
    ) -> Result<Self, (Self, DetachedAttachRefusal)> {
        let Self::Continuous(value) = self else {
            return Err((self, DetachedAttachRefusal::DeliveredMarkerAwaitingAck));
        };
        if !charged_churn_fits(episode_churn_used, delta_cycles, episode_churn_limit) {
            return Err((self, DetachedAttachRefusal::EpisodeChurnLimit));
        }
        if !is_next_generation(value.binding_epoch, new_binding_epoch) {
            return Err((self, DetachedAttachRefusal::StaleAuthority));
        }
        Ok(Self::Continuous(CursorProgressContinuous {
            binding_epoch: new_binding_epoch,
            ..value
        }))
    }

    /// Consumes exact live Leave and installs its measured clear/OP/PC result.
    ///
    /// # Errors
    ///
    /// Returns the unchanged owed state unless Leave names the exact live
    /// participant and binding epoch.
    pub fn leave(
        self,
        debt: ClosureDebt,
        event: Event,
        successor: DebtCompletion,
    ) -> Result<ClosureState, ClosureState> {
        let original = owed(debt, StoredEdge::ParticipantCursorProgress(self));
        let EventKind::LeaveCommitted {
            participant_id,
            authority: LeaveAuthority::Live(binding_epoch),
            ..
        } = event.0
        else {
            return Err(original);
        };
        if participant_id != self.participant_id() || binding_epoch != self.binding_epoch() {
            return Err(original);
        }
        Ok(successor.into_state())
    }
}

/// Crate-private failed proof mint carrying the same one-use marker record back
/// to its move-owned frontier.
pub(super) struct FencedAttachRecordRefusal {
    record: ValidatedMarkerRecord,
}

impl core::fmt::Debug for FencedAttachRecordRefusal {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str("FencedAttachRecordRefusal { record: <linear> }")
    }
}

impl FencedAttachRecordRefusal {
    /// Returns the exact record authority for serial reinstall and retry.
    pub(super) const fn into_record(self) -> ValidatedMarkerRecord {
        self.record
    }
}

impl DetachedCredentialRecovery {
    /// Validates exact-current K and exit claims for detached Leave.
    #[must_use]
    pub const fn validate_leave_claim(
        &self,
        participant_id: ParticipantId,
        actual_charge: ResourceVector,
        remaining_k: ResourceVector,
        exit_claims: u64,
    ) -> Option<KClaimBackedDetachedLeave> {
        validate_detached_claim(
            self.participant_id,
            DetachedClaimTarget::CredentialRecovery {
                marker_delivery_seq: self.marker_delivery_seq,
                binding_epoch: self.prior_binding_epoch,
            },
            participant_id,
            actual_charge,
            remaining_k,
            exit_claims,
        )
    }

    /// Consumes the sole fully validated marker occurrence and mints one fenced
    /// attach proof.
    ///
    /// This is deliberately visible only inside the lifecycle module. Public
    /// descriptive recovery values cannot invoke it without the private,
    /// non-cloneable record removed from a move-owned frontier.
    pub(super) fn fenced_attach(
        self,
        record_authority: ValidatedMarkerRecord,
        debt: ClosureDebt,
        event: Event,
        successor: DebtCompletion,
    ) -> Result<FencedAttachCommit, Box<FencedAttachRecordRefusal>> {
        let Some(proof) =
            FencedAttachCommit::restore_validated(self, &record_authority, debt, event, successor)
        else {
            return Err(Box::new(FencedAttachRecordRefusal {
                record: record_authority,
            }));
        };
        record_authority.consume();
        Ok(proof)
    }

    /// Consumes exact K-backed detached Leave and installs only clear/OP/PC.
    ///
    /// # Errors
    ///
    /// Returns the unchanged owed state unless the Leave and private claim
    /// authority are bound to this exact recovery edge.
    pub fn detached_leave(
        self,
        debt: ClosureDebt,
        event: Event,
        evidence: KClaimBackedDetachedLeave,
        successor: DebtCompletion,
    ) -> Result<ClosureState, ClosureState> {
        let original = owed(debt, StoredEdge::DetachedCredentialRecovery(self));
        let EventKind::LeaveCommitted {
            participant_id,
            authority: LeaveAuthority::Detached,
            ..
        } = event.0
        else {
            return Err(original);
        };
        let target = DetachedClaimTarget::CredentialRecovery {
            marker_delivery_seq: self.marker_delivery_seq,
            binding_epoch: self.prior_binding_epoch,
        };
        if participant_id != self.participant_id
            || evidence.participant_id != self.participant_id
            || evidence.target != target
        {
            return Err(original);
        }
        Ok(successor.into_state())
    }

    /// Ordinary non-fenced attach is refused without mutation.
    #[must_use]
    pub const fn ordinary_attach_refusal(self) -> DetachedAttachRefusal {
        let _ = self;
        DetachedAttachRefusal::RecoveryFence
    }

    /// An explicit marker is eligible only when it is the exact recovery marker.
    #[must_use]
    pub const fn marker_attach_refusal(
        self,
        presented_marker: DeliverySeq,
    ) -> Option<DetachedAttachRefusal> {
        if presented_marker == self.marker_delivery_seq {
            None
        } else {
            Some(DetachedAttachRefusal::MarkerMismatch)
        }
    }

    /// Authority supersession before commit preserves DCR and is stale.
    #[must_use]
    pub const fn authority_superseded(self) -> (Self, DetachedAttachRefusal) {
        (self, DetachedAttachRefusal::StaleAuthority)
    }

    /// Applies only an unrelated participant event, preserving this edge while
    /// debt remains or clearing it with debt.
    ///
    /// # Errors
    ///
    /// Returns the unchanged owed state for a non-participant event or an event
    /// owned by this detached participant.
    pub const fn unrelated_event(
        self,
        debt: ClosureDebt,
        event: Event,
        resulting_debt: Option<ClosureDebt>,
    ) -> Result<ClosureState, ClosureState> {
        unrelated_detached_event(
            StoredEdge::DetachedCredentialRecovery(self),
            self.participant_id,
            debt,
            event,
            resulting_debt,
        )
    }
}

mod sealed {
    pub trait Sealed {}
}

/// Sealed intended-dead-end contract for DMR and `DCursor`.
pub trait LeaveOnlyEdge: sealed::Sealed + Sized + Copy {
    /// Returns the exact owner of this Leave-only edge.
    fn participant_id(self) -> ParticipantId;

    /// Validates participant, exact target, actual charge, remaining K, and the
    /// available exit claim before creating private Leave authority.
    fn validate_leave_claim(
        &self,
        participant_id: ParticipantId,
        actual_charge: ResourceVector,
        remaining_k: ResourceVector,
        exit_claims: u64,
    ) -> Option<KClaimBackedDetachedLeave>;

    /// Sole successful owner transition: exact-current K-backed detached Leave.
    ///
    /// # Errors
    ///
    /// Returns the unchanged owed state unless the Leave and private claim
    /// authority both name this exact Leave-only edge.
    fn leave(
        self,
        debt: ClosureDebt,
        event: Event,
        evidence: KClaimBackedDetachedLeave,
        successor: DebtCompletion,
    ) -> Result<ClosureState, ClosureState>;

    /// Repeat exact fate is an event-consuming no-op.
    ///
    /// # Errors
    ///
    /// Returns the unchanged edge when the event is not fate for its exact owner
    /// and last dead binding epoch.
    fn repeat_fate(self, event: Event) -> Result<Self, Self>;

    /// Supersession is stale and preserves the exact edge.
    fn authority_superseded(self) -> (Self, DetachedAttachRefusal) {
        (self, DetachedAttachRefusal::StaleAuthority)
    }

    /// Normal/marker ack and ordinary admission have no binding authority.
    fn binding_required_refusal(self) -> DetachedAttachRefusal {
        let _ = self;
        DetachedAttachRefusal::NoBinding
    }

    /// Applies an unrelated participant event, preserving this edge while debt
    /// remains or clearing it with debt.
    ///
    /// # Errors
    ///
    /// Returns the unchanged owed state for a non-participant event or an event
    /// owned by this detached participant.
    fn unrelated_event(
        self,
        debt: ClosureDebt,
        event: Event,
        resulting_debt: Option<ClosureDebt>,
    ) -> Result<ClosureState, ClosureState>;
}

impl sealed::Sealed for DetachedMarkerRelease {}
impl sealed::Sealed for DetachedCursorRelease {}

impl LeaveOnlyEdge for DetachedMarkerRelease {
    fn participant_id(self) -> ParticipantId {
        self.participant_id
    }

    fn validate_leave_claim(
        &self,
        participant_id: ParticipantId,
        actual_charge: ResourceVector,
        remaining_k: ResourceVector,
        exit_claims: u64,
    ) -> Option<KClaimBackedDetachedLeave> {
        validate_detached_claim(
            self.participant_id,
            DetachedClaimTarget::MarkerRelease {
                marker_delivery_seq: self.marker_delivery_seq,
                binding_epoch: self.last_dead_binding_epoch,
            },
            participant_id,
            actual_charge,
            remaining_k,
            exit_claims,
        )
    }

    fn leave(
        self,
        debt: ClosureDebt,
        event: Event,
        evidence: KClaimBackedDetachedLeave,
        successor: DebtCompletion,
    ) -> Result<ClosureState, ClosureState> {
        let original = owed(debt, StoredEdge::DetachedMarkerRelease(self));
        let EventKind::LeaveCommitted {
            participant_id,
            authority: LeaveAuthority::Detached,
            ..
        } = event.0
        else {
            return Err(original);
        };
        let target = DetachedClaimTarget::MarkerRelease {
            marker_delivery_seq: self.marker_delivery_seq,
            binding_epoch: self.last_dead_binding_epoch,
        };
        if participant_id != self.participant_id
            || evidence.participant_id != self.participant_id
            || evidence.target != target
        {
            return Err(original);
        }
        Ok(successor.into_state())
    }

    fn repeat_fate(self, event: Event) -> Result<Self, Self> {
        let EventKind::BindingFateObserved {
            participant_id,
            binding_epoch,
            ..
        } = event.0
        else {
            return Err(self);
        };
        if participant_id == self.participant_id && binding_epoch == self.last_dead_binding_epoch {
            Ok(self)
        } else {
            Err(self)
        }
    }

    fn unrelated_event(
        self,
        debt: ClosureDebt,
        event: Event,
        resulting_debt: Option<ClosureDebt>,
    ) -> Result<ClosureState, ClosureState> {
        unrelated_detached_event(
            StoredEdge::DetachedMarkerRelease(self),
            self.participant_id,
            debt,
            event,
            resulting_debt,
        )
    }
}

impl DetachedMarkerRelease {
    /// Ordinary attach cannot cross this Leave-only edge.
    #[must_use]
    pub const fn ordinary_attach_refusal(self) -> DetachedAttachRefusal {
        let _ = self;
        DetachedAttachRefusal::RecoveryFence
    }

    /// The exact undelivered marker selects `MarkerNotDelivered`; another marker
    /// selects `MarkerMismatch` without fabricating an expected delivery fact.
    #[must_use]
    pub const fn marker_attach_refusal(
        self,
        presented_marker: DeliverySeq,
    ) -> DetachedAttachRefusal {
        if presented_marker == self.marker_delivery_seq {
            DetachedAttachRefusal::MarkerNotDelivered
        } else {
            DetachedAttachRefusal::MarkerMismatch
        }
    }
}

impl LeaveOnlyEdge for DetachedCursorRelease {
    fn participant_id(self) -> ParticipantId {
        self.participant_id
    }

    fn validate_leave_claim(
        &self,
        participant_id: ParticipantId,
        actual_charge: ResourceVector,
        remaining_k: ResourceVector,
        exit_claims: u64,
    ) -> Option<KClaimBackedDetachedLeave> {
        validate_detached_claim(
            self.participant_id,
            DetachedClaimTarget::CursorRelease {
                binding_epoch: self.last_dead_binding_epoch,
            },
            participant_id,
            actual_charge,
            remaining_k,
            exit_claims,
        )
    }

    fn leave(
        self,
        debt: ClosureDebt,
        event: Event,
        evidence: KClaimBackedDetachedLeave,
        successor: DebtCompletion,
    ) -> Result<ClosureState, ClosureState> {
        let original = owed(debt, StoredEdge::DetachedCursorRelease(self));
        let EventKind::LeaveCommitted {
            participant_id,
            authority: LeaveAuthority::Detached,
            ..
        } = event.0
        else {
            return Err(original);
        };
        let target = DetachedClaimTarget::CursorRelease {
            binding_epoch: self.last_dead_binding_epoch,
        };
        if participant_id != self.participant_id
            || evidence.participant_id != self.participant_id
            || evidence.target != target
        {
            return Err(original);
        }
        Ok(successor.into_state())
    }

    fn repeat_fate(self, event: Event) -> Result<Self, Self> {
        let EventKind::BindingFateObserved {
            participant_id,
            binding_epoch,
            ..
        } = event.0
        else {
            return Err(self);
        };
        if participant_id == self.participant_id && binding_epoch == self.last_dead_binding_epoch {
            Ok(self)
        } else {
            Err(self)
        }
    }

    fn unrelated_event(
        self,
        debt: ClosureDebt,
        event: Event,
        resulting_debt: Option<ClosureDebt>,
    ) -> Result<ClosureState, ClosureState> {
        unrelated_detached_event(
            StoredEdge::DetachedCursorRelease(self),
            self.participant_id,
            debt,
            event,
            resulting_debt,
        )
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

const fn owed(debt: ClosureDebt, edge: StoredEdge) -> ClosureState {
    ClosureState::Owed { debt, edge }
}

const fn preserve_or_clear(resulting_debt: Option<ClosureDebt>, edge: StoredEdge) -> ClosureState {
    match resulting_debt {
        Some(debt) => owed(debt, edge),
        None => ClosureState::Clear,
    }
}

const fn projection_completion_boundary(
    edge: ObserverProjection,
    event: Event,
) -> Option<DeliverySeq> {
    let EventKind::ProjectionCompleted { through_seq } = event.0 else {
        return None;
    };
    if through_seq == edge.through_seq {
        Some(through_seq)
    } else {
        None
    }
}

const fn physical_completion_floor(edge: PhysicalCompaction, event: Event) -> Option<DeliverySeq> {
    match event.0 {
        EventKind::CompactionCompleted {
            from_floor,
            through_seq,
            resulting_floor,
        } if from_floor == edge.from_floor
            && through_seq == edge.through_seq
            && resulting_floor > edge.through_seq =>
        {
            Some(resulting_floor)
        }
        _ => None,
    }
}

const fn progress_event_floor(event: Event) -> Option<DeliverySeq> {
    match event.0 {
        EventKind::CursorProgressed {
            resulting_floor, ..
        }
        | EventKind::BindingFateObserved {
            resulting_floor, ..
        }
        | EventKind::LeaveCommitted {
            resulting_floor, ..
        } => Some(resulting_floor),
        _ => None,
    }
}

const fn cursor_event_boundary(event: Event) -> DeliverySeq {
    match event.0 {
        EventKind::CursorProgressed {
            progress: CursorProgressEvent::Normal { through_seq, .. },
            ..
        } => through_seq,
        _ => 0,
    }
}

fn greater_ack_matches(edge: ParticipantCursorProgress, event: Event) -> bool {
    let EventKind::CursorProgressed {
        participant_id,
        binding_epoch,
        progress:
            CursorProgressEvent::Normal {
                previous_cursor,
                through_seq,
            },
        ..
    } = event.0
    else {
        return false;
    };
    participant_id == edge.participant_id()
        && binding_epoch == edge.binding_epoch()
        && previous_cursor < edge.through_seq()
        && through_seq > edge.through_seq()
}

const fn strict_edge_matches_boundary(edge: StoredEdge, boundary: DeliverySeq) -> bool {
    match edge {
        StoredEdge::ObserverProjection(value) => value.through_seq == boundary,
        StoredEdge::PhysicalCompaction(value) => value.through_seq == boundary,
        StoredEdge::MarkerDelivery(value) => value.marker_delivery_seq == boundary,
        StoredEdge::ParticipantCursorProgress(value) => value.through_seq() == boundary,
        StoredEdge::DetachedCredentialRecovery(_) => false,
        StoredEdge::DetachedMarkerRelease(value) => value.marker_delivery_seq == boundary,
        // DCursor has no sequence field in the frozen tag. The explicit boundary
        // supplied to the predecessor-bound successor is its typed causal-order
        // witness under LP-EXTRACTION-GOAL.md Fix 2.
        StoredEdge::DetachedCursorRelease(_) => true,
    }
}

const fn charged_churn_fits(used: u64, delta: u64, limit: u64) -> bool {
    delta > 0 && widen_u64(used) + widen_u64(delta) <= widen_u64(limit)
}

#[allow(clippy::cast_lossless)]
const fn widen_u64(value: u64) -> u128 {
    value as u128
}

const fn is_next_generation(old: BindingEpoch, new: BindingEpoch) -> bool {
    match old.capability_generation.get().checked_add(1) {
        Some(expected) => new.capability_generation.get() == expected,
        None => false,
    }
}

const fn validate_detached_claim(
    owner: ParticipantId,
    target: DetachedClaimTarget,
    participant_id: ParticipantId,
    actual_charge: ResourceVector,
    remaining_k: ResourceVector,
    exit_claims: u64,
) -> Option<KClaimBackedDetachedLeave> {
    if participant_id != owner
        || actual_charge.entries == 0
        || actual_charge.bytes == 0
        || actual_charge.entries > remaining_k.entries
        || actual_charge.bytes > remaining_k.bytes
        || exit_claims == 0
    {
        return None;
    }
    Some(KClaimBackedDetachedLeave {
        participant_id,
        target,
        actual_charge,
    })
}

const fn event_participant(event: Event) -> Option<ParticipantId> {
    match event.0 {
        EventKind::MarkerDelivered { participant_id, .. }
        | EventKind::CursorProgressed { participant_id, .. }
        | EventKind::BindingFateObserved { participant_id, .. }
        | EventKind::LeaveCommitted { participant_id, .. }
        | EventKind::FencedRecoveryCommitted { participant_id, .. } => Some(participant_id),
        EventKind::ProjectionCompleted { .. }
        | EventKind::CompactionCompleted { .. }
        | EventKind::MarkerAppended { .. } => None,
    }
}

const fn unrelated_detached_event(
    edge: StoredEdge,
    owner: ParticipantId,
    debt: ClosureDebt,
    event: Event,
    resulting_debt: Option<ClosureDebt>,
) -> Result<ClosureState, ClosureState> {
    let original = owed(debt, edge);
    let Some(participant_id) = event_participant(event) else {
        return Err(original);
    };
    if participant_id == owner {
        return Err(original);
    }
    Ok(preserve_or_clear(resulting_debt, edge))
}
