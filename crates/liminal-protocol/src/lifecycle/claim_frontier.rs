//! Exact reserved sequence and transaction-order claim frontiers.
//!
//! `docs/design/LP-EXTRACTION-GOAL.md` Fix 2 deliberately excludes the frozen
//! occurrence-array layout. This module keeps participant-keyed facts and
//! compact product range descriptors instead: exact direct/candidate/recovery
//! positions use O(I) bounded storage, while product handles are derived lazily
//! from active-identity ranks and are never expanded into an `I x I` array.

use alloc::{boxed::Box, vec::Vec};

use crate::{
    algebra::ResourceVector,
    outcome::{CandidatePhase, ClaimCounter, ParticipantStateCorruptReason},
    wire::{
        BindingEpoch, ClosureCheckedEnvelope, ConversationId, DeliverySeq, ParticipantId,
        TransactionOrder,
    },
};

use super::{
    AttachedLifecycleRecord, BindingOrigin, BindingState, ClosureAccounting, ClosureState,
    CommittedBindingTerminal, Event, InitialEnrollmentClosureProjection,
    InitialEnrollmentOperationCommit, LeaveCommitError, LiveMember, MarkerDelivery,
    ObserverCheckedOperation, ObserverFloorDecision, ObserverProjection, OrderClaims, OrderHigh,
    OrderLedger, ParticipantCursorProgress, PendingFinalization, PreparedLeaveAuthority,
    RecoveryQuartetStatus, RecoverySequenceReserve, RemainingClosureDecision, SequenceClaims,
    SequenceLedger, StoredEdge, check_observer_floor, check_remaining_closure,
    operations::ordinary_record_projection::{
        OrdinaryFixedPointPlan, OrdinaryProjectionError, OrdinaryProjectionFacts,
        OrdinaryProjectionKernelDecision, OrdinaryRecordDrainFirst,
        OrdinaryRecordProjectionDecision, OrdinaryRecordProjectionFailure,
        OrdinaryRecordProjectionInput, ProjectedOrdinaryRecord, project_ordinary_fixed_point,
    },
};

/// Counter whose frontier failed restoration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClaimFrontierCounter {
    /// Conversation delivery sequence.
    DeliverySequence,
    /// Conversation transaction-order major.
    TransactionOrder,
}

/// Structural reason for a claim-frontier failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClaimFrontierInvalidReason {
    /// Numeric positions contain a gap, duplicate, collision, or invalid bound.
    NumericPosition,
    /// One immutable candidate key is duplicated or malformed.
    CandidateKey,
    /// One logical owner is unknown, duplicated, missing, or in the wrong class.
    LogicalOwner,
    /// A product range is misordered or has the wrong active-rank extent.
    ProductRange,
    /// A recovery block is torn, non-adjacent, or inconsistent across counters.
    RecoveryBlock,
    /// Exact frontier owner counts disagree with the aggregate ledger.
    AggregateLedger,
}

/// Deterministic claim-frontier restoration failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClaimFrontierError {
    /// Counter whose union failed.
    pub counter: ClaimFrontierCounter,
    /// Lowest checked-u128 index selected by the frozen scan.
    pub first_bad_position: u128,
    /// Structural class of the first failure.
    pub reason: ClaimFrontierInvalidReason,
}

/// Exact live participant binding state used by marker planning.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrontierBinding {
    /// Participant is bound to this exact current epoch.
    Bound(BindingEpoch),
    /// Participant is detached after this exact last authoritative epoch.
    Detached(BindingEpoch),
}

/// Participant-indexed membership fact used by claim validation and planning.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrontierParticipant {
    participant_index: ParticipantId,
    cursor: DeliverySeq,
    binding: FrontierBinding,
}

impl FrontierParticipant {
    /// Creates one exact live-membership frontier fact.
    #[must_use]
    pub const fn new(
        participant_index: ParticipantId,
        cursor: DeliverySeq,
        binding: FrontierBinding,
    ) -> Self {
        Self {
            participant_index,
            cursor,
            binding,
        }
    }

    /// Returns the permanent participant index.
    #[must_use]
    pub const fn participant_index(self) -> ParticipantId {
        self.participant_index
    }

    /// Returns the durable cumulative cursor.
    #[must_use]
    pub const fn cursor(self) -> DeliverySeq {
        self.cursor
    }

    /// Returns the exact current or last authoritative binding state.
    #[must_use]
    pub const fn binding(self) -> FrontierBinding {
        self.binding
    }
}

/// Sorted unique permanent indexes of current live members.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActiveIdentityRanks {
    participants: Vec<FrontierParticipant>,
}

impl ActiveIdentityRanks {
    /// Validates the signed identity-slot bound, ascending unique indexes, and
    /// cursors at or below `H`.
    ///
    /// # Errors
    ///
    /// Returns a delivery-sequence [`ClaimFrontierError`] at the first malformed
    /// rank. The rank itself is the deterministic bad-position index.
    pub fn try_new(
        participants: Vec<FrontierParticipant>,
        high_watermark: DeliverySeq,
        identity_slot_limit: u64,
    ) -> Result<Self, ClaimFrontierError> {
        if usize_to_u128(participants.len()) > u128::from(identity_slot_limit) {
            return Err(sequence_error(
                u128::from(identity_slot_limit),
                ClaimFrontierInvalidReason::LogicalOwner,
            ));
        }
        let mut previous = None;
        for (rank, participant) in participants.iter().enumerate() {
            if previous.is_some_and(|value| value >= participant.participant_index)
                || participant.participant_index >= identity_slot_limit
                || participant.cursor > high_watermark
            {
                return Err(sequence_error(
                    rank_index(rank),
                    ClaimFrontierInvalidReason::LogicalOwner,
                ));
            }
            previous = Some(participant.participant_index);
        }
        Ok(Self { participants })
    }

    /// Borrows the ascending live participant facts.
    #[must_use]
    pub fn participants(&self) -> &[FrontierParticipant] {
        &self.participants
    }

    /// Returns the number of active identity ranks.
    #[must_use]
    pub fn len(&self) -> u64 {
        usize_to_u64(self.participants.len())
    }

    /// Returns whether no live identity rank exists.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.participants.is_empty()
    }

    fn contains(&self, participant_index: ParticipantId) -> bool {
        self.participants
            .binary_search_by_key(&participant_index, |participant| {
                participant.participant_index
            })
            .is_ok()
    }
}

/// Exact active binding-terminal claim authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BindingTerminalOwner {
    /// Permanent participant index.
    pub participant_index: ParticipantId,
    /// Binding epoch whose future fate owns this claim.
    pub binding_epoch: BindingEpoch,
}

/// Direct sequence-claim owner stored in an identity slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SequenceDirectOwner {
    /// Eventual tokenized `Left` record.
    MembershipExit {
        /// Permanent participant index.
        participant_index: ParticipantId,
    },
    /// Future terminal for one exact active binding.
    BindingTerminal(BindingTerminalOwner),
}

/// One exact movable direct sequence claim.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MovableSequenceClaim {
    /// Owned delivery-sequence position.
    pub delivery_seq: DeliverySeq,
    /// Exact identity-slot owner.
    pub owner: SequenceDirectOwner,
}

/// Terminal source retained by immutable marker provenance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TerminalProductSource {
    /// Ordinary active-binding terminal claim.
    Binding(BindingTerminalOwner),
    /// Edge-owned replacement terminal for a recovered binding.
    RecoveryReplacement {
        /// Permanent participant index.
        participant_index: ParticipantId,
        /// Exact prospective replacement epoch.
        binding_epoch: BindingEpoch,
    },
}

impl TerminalProductSource {
    /// Names the prospective replacement terminal produced by recovery.
    #[must_use]
    pub const fn recovery_replacement(
        participant_index: ParticipantId,
        binding_epoch: BindingEpoch,
    ) -> Self {
        Self::RecoveryReplacement {
            participant_index,
            binding_epoch,
        }
    }
}

/// Immutable origin of one planned or appended marker value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarkerProvenance {
    /// Marker was planned directly by an optional floor transition.
    NonProductM,
    /// Marker was conditionally reserved by a terminal product.
    TerminalProduct {
        /// Exact causal terminal source.
        terminal: TerminalProductSource,
        /// Permanent affected participant index.
        affected_participant: ParticipantId,
    },
    /// Marker was conditionally reserved by a membership-exit product.
    ExitProduct {
        /// Exiting participant whose E claim caused the product.
        exit_participant: ParticipantId,
        /// Remaining participant the possible marker protects.
        remaining_participant: ParticipantId,
    },
}

impl MarkerProvenance {
    /// Names one conditional terminal-product marker provenance.
    #[must_use]
    pub const fn terminal_product(
        terminal: TerminalProductSource,
        affected_participant: ParticipantId,
    ) -> Self {
        Self::TerminalProduct {
            terminal,
            affected_participant,
        }
    }

    /// Names one conditional membership-exit-product marker provenance.
    #[must_use]
    pub const fn exit_product(
        exit_participant: ParticipantId,
        remaining_participant: ParticipantId,
    ) -> Self {
        Self::ExitProduct {
            exit_participant,
            remaining_participant,
        }
    }
}

/// Exact typed body class for one retained causal record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetainedCausalRecordKind {
    /// Retained binding terminal with exact epoch ownership.
    BindingTerminal(BindingTerminalOwner),
    /// Retained tokenized membership exit.
    MembershipExit {
        /// Permanent exiting participant.
        participant_index: ParticipantId,
    },
    /// Retained attached lifecycle record.
    AttachLifecycle {
        /// Permanent affected participant.
        participant_index: ParticipantId,
        /// Exact attached binding epoch.
        binding_epoch: BindingEpoch,
    },
    /// Retained ordinary application record.
    OrdinaryRecord {
        /// Permanent verified sender.
        participant_index: ParticipantId,
    },
    /// Retained compaction marker with immutable provenance.
    CompactionMarker {
        /// Permanent marker owner.
        participant_index: ParticipantId,
        /// Immutable marker origin.
        provenance: MarkerProvenance,
    },
}

/// One typed retained record fact used for candidate-key and provenance checks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetainedCausalRecord {
    /// Durable appended sequence.
    pub delivery_seq: DeliverySeq,
    /// Complete immutable candidate/direct-record key.
    pub admission_order: super::AdmissionOrder,
    /// Typed retained record body facts.
    pub kind: RetainedCausalRecordKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HistoricalCausalKind {
    BindingTerminal(BindingTerminalOwner),
    MembershipExit(ParticipantId),
}

/// Raw durable facts for one compacted causal lifecycle record.
///
/// This is storage input, not executable provenance. Only complete
/// [`super::ParticipantConversationRestore`] validation can pair these facts
/// with owned membership or tombstone history and turn them into crate-private
/// marker provenance. In particular, neither a raw binding-terminal position
/// nor a raw `Left` major can be converted into sealed authority directly.
///
/// ```compile_fail
/// use liminal_protocol::lifecycle::HistoricalCausalAuthority;
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HistoricalCausalFactRestore {
    /// A compacted binding terminal retained by participant history.
    BindingTerminal {
        /// Owning conversation.
        conversation_id: ConversationId,
        /// Permanent participant index.
        participant_index: ParticipantId,
        /// Exact ended binding epoch.
        binding_epoch: BindingEpoch,
        /// Immutable binding-terminal tuple.
        admission_order: super::AdmissionOrder,
    },
    /// A compacted `Left` record retained by the permanent tombstone.
    MembershipExit {
        /// Owning conversation.
        conversation_id: ConversationId,
        /// Permanent retired participant index.
        participant_index: ParticipantId,
        /// Immutable membership-exit tuple.
        admission_order: super::AdmissionOrder,
    },
}

/// Raw immutable fact that one retained marker was durably delivered to an
/// exact historical binding epoch.
///
/// This fact is distinct from current marker-anchor ownership and current
/// membership. It may therefore outlive fenced attach and multiple later
/// binding cycles, and several retained markers may name the same participant.
/// Only joint conversation restoration can turn it into executable recovery
/// provenance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HistoricalMarkerDeliveryFactRestore {
    /// Owning conversation.
    pub conversation_id: ConversationId,
    /// Permanent marker owner.
    pub participant_index: ParticipantId,
    /// Exact retained marker record.
    pub marker_delivery_seq: DeliverySeq,
    /// Binding epoch to which that marker was durably delivered.
    pub delivered_binding_epoch: BindingEpoch,
}

/// Crate-sealed compacted-history authority derived during total restoration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct HistoricalCausalAuthority {
    conversation_id: ConversationId,
    admission_order: super::AdmissionOrder,
    kind: HistoricalCausalKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct HistoricalMarkerDeliveryAuthority {
    participant_index: ParticipantId,
    marker_delivery_seq: DeliverySeq,
    delivered_binding_epoch: BindingEpoch,
}

impl HistoricalCausalAuthority {
    pub(super) const fn from_committed_terminal(terminal: super::CommittedBindingTerminal) -> Self {
        Self {
            conversation_id: terminal.conversation_id(),
            admission_order: terminal.admission_order(),
            kind: HistoricalCausalKind::BindingTerminal(BindingTerminalOwner {
                participant_index: terminal.participant_id(),
                binding_epoch: terminal.binding_epoch(),
            }),
        }
    }

    pub(super) const fn from_retired<EF, V, LF>(
        retired: &super::RetiredIdentity<EF, V, LF>,
    ) -> Self {
        Self {
            conversation_id: retired.conversation_id(),
            admission_order: retired.left_admission_order(),
            kind: HistoricalCausalKind::MembershipExit(retired.participant_id()),
        }
    }

    const fn from_restore(fact: HistoricalCausalFactRestore) -> Self {
        match fact {
            HistoricalCausalFactRestore::BindingTerminal {
                conversation_id,
                participant_index,
                binding_epoch,
                admission_order,
            } => Self {
                conversation_id,
                admission_order,
                kind: HistoricalCausalKind::BindingTerminal(BindingTerminalOwner {
                    participant_index,
                    binding_epoch,
                }),
            },
            HistoricalCausalFactRestore::MembershipExit {
                conversation_id,
                participant_index,
                admission_order,
            } => Self {
                conversation_id,
                admission_order,
                kind: HistoricalCausalKind::MembershipExit(participant_index),
            },
        }
    }
}

#[derive(Debug)]
pub(super) struct ValidatedConversationHistory {
    causal_authorities: Vec<HistoricalCausalAuthority>,
    binding_origins: Vec<BindingOrigin>,
    total: bool,
}

impl ValidatedConversationHistory {
    pub(super) const fn empty() -> Self {
        Self {
            causal_authorities: Vec::new(),
            binding_origins: Vec::new(),
            total: false,
        }
    }

    pub(super) const fn new(
        causal_authorities: Vec<HistoricalCausalAuthority>,
        binding_origins: Vec<BindingOrigin>,
    ) -> Self {
        Self {
            causal_authorities,
            binding_origins,
            total: true,
        }
    }

    pub(super) fn ordinary_origin(
        &self,
        conversation_id: ConversationId,
        participant_index: ParticipantId,
        binding_epoch: BindingEpoch,
    ) -> Option<&BindingOrigin> {
        self.binding_origins.iter().find(|origin| {
            origin.conversation_id() == conversation_id
                && origin.participant_id() == participant_index
                && origin.binding_epoch() == binding_epoch
                && origin.is_unfenced()
        })
    }
}

/// Product class that may own a value before its causal transaction fires.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SequenceProductClass {
    /// `L x T`.
    LiveTimesTerminal,
    /// `L x RT`.
    LiveTimesReplacementTerminal,
    /// `L_other x E`.
    OtherLiveTimesExit,
}

/// Current logical owner of one marker-provenance value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarkerSequenceOwner {
    /// Required-but-unwritten marker claim `M`.
    Marker,
    /// An unfired conditional product range.
    ConditionalProduct(SequenceProductClass),
}

/// Complete immutable marker candidate authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MarkerCandidateAuthority {
    /// Exact assigned marker sequence.
    pub delivery_seq: DeliverySeq,
    /// Exact phase-4 causal candidate key.
    pub admission_order: super::AdmissionOrder,
    /// Current or last authoritative delivery target.
    pub target_binding: FrontierBinding,
    /// Immutable marker provenance.
    pub provenance: MarkerProvenance,
    /// Last sequence acknowledged before this participant was overtaken.
    pub abandoned_after: DeliverySeq,
    /// Last pre-marker sequence abandoned by this marker decision.
    pub abandoned_through: DeliverySeq,
    /// Physical retained floor selected by this marker decision.
    pub physical_floor_at_decision: DeliverySeq,
    /// Current sequence owner; an immutable candidate must own `M`.
    pub current_owner: MarkerSequenceOwner,
}

/// Immutable assigned candidate above the current sequence high watermark.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImmutableSequenceCandidate {
    /// Pending exact binding terminal.
    BindingTerminal {
        /// Assigned delivery sequence.
        delivery_seq: DeliverySeq,
        /// Exact causal candidate key.
        admission_order: super::AdmissionOrder,
        /// Binding-terminal authority consumed into the candidate.
        owner: BindingTerminalOwner,
    },
    /// Planned phase-4 compaction marker.
    Marker(MarkerCandidateAuthority),
}

impl ImmutableSequenceCandidate {
    /// Returns the immutable assigned delivery sequence.
    #[must_use]
    pub const fn delivery_seq(self) -> DeliverySeq {
        match self {
            Self::BindingTerminal { delivery_seq, .. } => delivery_seq,
            Self::Marker(candidate) => candidate.delivery_seq,
        }
    }

    /// Returns the complete causal candidate key.
    #[must_use]
    pub const fn admission_order(self) -> super::AdmissionOrder {
        match self {
            Self::BindingTerminal {
                admission_order, ..
            } => admission_order,
            Self::Marker(candidate) => candidate.admission_order,
        }
    }
}

/// Persisted `L x T` product-range descriptor supplied during restoration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TerminalProductRangeRestore {
    /// First owned sequence value.
    pub start: DeliverySeq,
    /// Persisted range length, which must equal the active-rank count.
    pub length: u64,
    /// Exact terminal claim that owns the product row.
    pub terminal: BindingTerminalOwner,
}

/// Persisted `L x RT` product-range descriptor supplied during restoration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplacementTerminalProductRangeRestore {
    /// First owned sequence value.
    pub start: DeliverySeq,
    /// Persisted range length, which must equal the active-rank count.
    pub length: u64,
}

/// Persisted `L_other x E` product-range descriptor supplied during restoration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExitProductRangeRestore {
    /// First owned sequence value.
    pub start: DeliverySeq,
    /// Persisted range length, which must equal `L - 1`.
    pub length: u64,
    /// Permanent participant whose exit claim owns the product row.
    pub exit_participant: ParticipantId,
}

/// Compact persisted product descriptors supplied during restoration.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SequenceProductRangesRestore {
    /// One full active-rank range for every current terminal claim.
    pub live_times_terminal: Vec<TerminalProductRangeRestore>,
    /// The sole replacement-terminal range when the DCR pair exists.
    pub live_times_replacement_terminal: Option<ReplacementTerminalProductRangeRestore>,
    /// One all-other-active-ranks range for every current exit claim.
    pub other_live_times_exit: Vec<ExitProductRangeRestore>,
}

/// Validated compact `L x T` product range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TerminalProductRange {
    start: DeliverySeq,
    length: u64,
    terminal: BindingTerminalOwner,
}

impl TerminalProductRange {
    /// Returns the first owned sequence value.
    #[must_use]
    pub const fn start(self) -> DeliverySeq {
        self.start
    }

    /// Returns the terminal claim that owns this product row.
    #[must_use]
    pub const fn terminal(self) -> BindingTerminalOwner {
        self.terminal
    }

    /// Returns the validated active-rank extent.
    #[must_use]
    pub const fn length(self) -> u64 {
        self.length
    }

    /// Derives the owned value for one active-rank index without expanding the row.
    #[must_use]
    pub fn value_at_rank(self, active_rank: usize) -> Option<DeliverySeq> {
        if usize_to_u128(active_rank) >= u128::from(self.length) {
            return None;
        }
        checked_rank_value(self.start, active_rank)
    }
}

/// Validated compact `L x RT` product range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplacementTerminalProductRange {
    start: DeliverySeq,
    length: u64,
    participant_index: ParticipantId,
    marker_delivery_seq: DeliverySeq,
    prior_binding_epoch: BindingEpoch,
}

impl ReplacementTerminalProductRange {
    /// Returns the first owned sequence value.
    #[must_use]
    pub const fn start(self) -> DeliverySeq {
        self.start
    }

    /// Returns the validated active-rank extent.
    #[must_use]
    pub const fn length(self) -> u64 {
        self.length
    }

    /// Returns the participant whose prospective replacement owns `RT`.
    #[must_use]
    pub const fn participant_index(self) -> ParticipantId {
        self.participant_index
    }

    /// Returns the delivered marker whose recovery owns `RT`.
    #[must_use]
    pub const fn marker_delivery_seq(self) -> DeliverySeq {
        self.marker_delivery_seq
    }

    /// Returns the prior authoritative epoch fenced by recovery.
    #[must_use]
    pub const fn prior_binding_epoch(self) -> BindingEpoch {
        self.prior_binding_epoch
    }

    /// Derives the owned value for one active-rank index without expanding the row.
    #[must_use]
    pub fn value_at_rank(self, active_rank: usize) -> Option<DeliverySeq> {
        if usize_to_u128(active_rank) >= u128::from(self.length) {
            return None;
        }
        checked_rank_value(self.start, active_rank)
    }
}

/// Validated compact `L_other x E` product range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExitProductRange {
    start: DeliverySeq,
    length: u64,
    exit_participant: ParticipantId,
}

impl ExitProductRange {
    /// Returns the first owned sequence value.
    #[must_use]
    pub const fn start(self) -> DeliverySeq {
        self.start
    }

    /// Returns the exit-claim owner.
    #[must_use]
    pub const fn exit_participant(self) -> ParticipantId {
        self.exit_participant
    }

    /// Returns the validated all-other-rank extent.
    #[must_use]
    pub const fn length(self) -> u64 {
        self.length
    }

    /// Derives the value for an affected active rank.
    ///
    /// The exiting identity is skipped, so no `I x I` expansion is formed.
    #[must_use]
    pub fn value_for_affected_rank(
        self,
        active_identities: &ActiveIdentityRanks,
        affected_rank: usize,
    ) -> Option<DeliverySeq> {
        let affected = active_identities.participants().get(affected_rank)?;
        if usize_to_u128(affected_rank) >= usize_to_u128(active_identities.participants.len()) {
            return None;
        }
        if affected.participant_index == self.exit_participant {
            return None;
        }
        let exit_rank = active_identities
            .participants()
            .binary_search_by_key(&self.exit_participant, |participant| {
                participant.participant_index
            })
            .ok()?;
        let compact_rank = if affected_rank < exit_rank {
            affected_rank
        } else {
            affected_rank.checked_sub(1)?
        };
        if usize_to_u128(compact_rank) >= u128::from(self.length) {
            return None;
        }
        checked_rank_value(self.start, compact_rank)
    }
}

/// Validated O(I) product descriptors for the sequence frontier.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SequenceProductRanges {
    live_times_terminal: Vec<TerminalProductRange>,
    live_times_replacement_terminal: Option<ReplacementTerminalProductRange>,
    other_live_times_exit: Vec<ExitProductRange>,
}

impl SequenceProductRanges {
    /// Borrows the `L x T` rows in terminal-owner order.
    #[must_use]
    pub fn live_times_terminal(&self) -> &[TerminalProductRange] {
        &self.live_times_terminal
    }

    /// Returns the optional `L x RT` row.
    #[must_use]
    pub const fn live_times_replacement_terminal(&self) -> Option<ReplacementTerminalProductRange> {
        self.live_times_replacement_terminal
    }

    /// Borrows the `L_other x E` rows in exit-owner order.
    #[must_use]
    pub fn other_live_times_exit(&self) -> &[ExitProductRange] {
        &self.other_live_times_exit
    }
}

/// Optional leading `T` member of a persisted DCR sequence interval.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoverySequenceTerminalRestore {
    /// Owned sequence value immediately before `RS`.
    pub delivery_seq: DeliverySeq,
    /// Exact active binding-terminal owner.
    pub owner: BindingTerminalOwner,
}

/// Public persisted shape of the sole DCR sequence interval.
///
/// `terminal` is present before its `T` claim materializes. Restoration accepts
/// individual persisted positions so a torn interval can be diagnosed, then
/// stores the validated interval as one indivisible value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoverySequenceBlockRestore {
    /// Optional leading active-terminal claim.
    pub terminal: Option<RecoverySequenceTerminalRestore>,
    /// Exact `RS` recovery-attach position.
    pub recovery_attach_seq: DeliverySeq,
    /// Exact adjacent `RT` replacement-terminal position.
    pub replacement_terminal_seq: DeliverySeq,
}

/// Validated indivisible DCR sequence interval.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoverySequenceBlock {
    terminal: Option<RecoverySequenceTerminalRestore>,
    recovery_attach_seq: DeliverySeq,
    replacement_terminal_seq: DeliverySeq,
    participant_index: ParticipantId,
    marker_delivery_seq: DeliverySeq,
    recovered_binding_epoch: BindingEpoch,
}

impl RecoverySequenceBlock {
    /// Returns the optional leading active-terminal claim.
    #[must_use]
    pub const fn terminal(self) -> Option<RecoverySequenceTerminalRestore> {
        self.terminal
    }

    /// Returns the exact `RS` position.
    #[must_use]
    pub const fn recovery_attach_seq(self) -> DeliverySeq {
        self.recovery_attach_seq
    }

    /// Returns the exact adjacent `RT` position.
    #[must_use]
    pub const fn replacement_terminal_seq(self) -> DeliverySeq {
        self.replacement_terminal_seq
    }

    /// Returns the participant recovered by the block.
    #[must_use]
    pub const fn participant_index(self) -> ParticipantId {
        self.participant_index
    }

    /// Returns the exact delivered marker fenced by recovery.
    #[must_use]
    pub const fn marker_delivery_seq(self) -> DeliverySeq {
        self.marker_delivery_seq
    }

    /// Returns the old binding epoch fenced by recovery.
    #[must_use]
    pub const fn recovered_binding_epoch(self) -> BindingEpoch {
        self.recovered_binding_epoch
    }
}

/// Public persisted input for sequence-frontier restoration.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SequenceClaimFrontierRestore {
    /// Exact movable identity-slot sequence claims.
    pub movable_claims: Vec<MovableSequenceClaim>,
    /// Immutable pending terminal and marker candidates.
    pub immutable_candidates: Vec<ImmutableSequenceCandidate>,
    /// Compact conditional product ranges.
    pub products: SequenceProductRangesRestore,
    /// The sole optional DCR interval.
    pub recovery: Option<RecoverySequenceBlockRestore>,
}

/// Validated exact sequence claim frontier.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SequenceClaimFrontier {
    ledger: SequenceLedger,
    movable_claims: Vec<MovableSequenceClaim>,
    immutable_candidates: Vec<ImmutableSequenceCandidate>,
    products: SequenceProductRanges,
    recovery: Option<RecoverySequenceBlock>,
}

impl SequenceClaimFrontier {
    /// Returns the aggregate ledger validated against these exact owners.
    #[must_use]
    pub const fn ledger(&self) -> SequenceLedger {
        self.ledger
    }

    /// Borrows exact movable identity-slot claims.
    #[must_use]
    pub fn movable_claims(&self) -> &[MovableSequenceClaim] {
        &self.movable_claims
    }

    /// Borrows immutable candidates in delivery-sequence order.
    #[must_use]
    pub fn immutable_candidates(&self) -> &[ImmutableSequenceCandidate] {
        &self.immutable_candidates
    }

    /// Borrows compact conditional product ranges.
    #[must_use]
    pub const fn products(&self) -> &SequenceProductRanges {
        &self.products
    }

    /// Returns the sole validated DCR sequence interval.
    #[must_use]
    pub const fn recovery(&self) -> Option<RecoverySequenceBlock> {
        self.recovery
    }
}

/// Direct movable transaction-order owner stored in a bounded identity slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderDirectOwner {
    /// Future terminal for one exact active binding (`A`).
    ActiveBindingTerminal(BindingTerminalOwner),
    /// Future tokenized `Left` for one live member (`X`).
    MembershipExit {
        /// Permanent participant index.
        participant_index: ParticipantId,
    },
}

/// One exact movable transaction-order claim.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MovableOrderClaim {
    /// Owned major.
    pub transaction_order: TransactionOrder,
    /// Exact bounded-slot owner.
    pub owner: OrderDirectOwner,
}

/// Public persisted candidate-major group supplied during restoration.
///
/// Several candidate keys caused by one transaction contribute one numeric
/// major to the frontier union.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImmutableOrderCandidateMajorRestore {
    /// Immutable assigned major.
    pub transaction_order: TransactionOrder,
    /// Complete candidate keys sharing that major.
    pub candidate_keys: Vec<super::AdmissionOrder>,
}

/// Validated immutable candidate-major group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImmutableOrderCandidateMajor {
    transaction_order: TransactionOrder,
    candidate_keys: Vec<super::AdmissionOrder>,
}

impl ImmutableOrderCandidateMajor {
    /// Returns the immutable assigned major.
    #[must_use]
    pub const fn transaction_order(&self) -> TransactionOrder {
        self.transaction_order
    }

    /// Borrows the complete candidate keys in canonical tuple order.
    #[must_use]
    pub fn candidate_keys(&self) -> &[super::AdmissionOrder] {
        &self.candidate_keys
    }
}

/// Optional leading `A` member of a persisted DCR order interval.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveryOrderActiveBindingRestore {
    /// Owned major immediately before `RO`.
    pub transaction_order: TransactionOrder,
    /// Exact active binding-terminal owner.
    pub owner: BindingTerminalOwner,
}

/// Public persisted shape of the sole DCR order interval.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveryOrderBlockRestore {
    /// Optional leading active-binding claim before its terminal materializes.
    pub active_binding: Option<RecoveryOrderActiveBindingRestore>,
    /// Exact `RO` recovery-operation major.
    pub recovery_operation_order: TransactionOrder,
    /// Exact adjacent `RA` replacement-terminal major.
    pub replacement_terminal_order: TransactionOrder,
}

/// Validated indivisible DCR order interval.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveryOrderBlock {
    active_binding: Option<RecoveryOrderActiveBindingRestore>,
    recovery_operation_order: TransactionOrder,
    replacement_terminal_order: TransactionOrder,
    participant_index: ParticipantId,
    marker_delivery_seq: DeliverySeq,
    recovered_binding_epoch: BindingEpoch,
}

impl RecoveryOrderBlock {
    /// Returns the optional leading active-binding claim.
    #[must_use]
    pub const fn active_binding(self) -> Option<RecoveryOrderActiveBindingRestore> {
        self.active_binding
    }

    /// Returns the exact `RO` major.
    #[must_use]
    pub const fn recovery_operation_order(self) -> TransactionOrder {
        self.recovery_operation_order
    }

    /// Returns the exact adjacent `RA` major.
    #[must_use]
    pub const fn replacement_terminal_order(self) -> TransactionOrder {
        self.replacement_terminal_order
    }

    /// Returns the participant recovered by the block.
    #[must_use]
    pub const fn participant_index(self) -> ParticipantId {
        self.participant_index
    }

    /// Returns the exact delivered marker fenced by recovery.
    #[must_use]
    pub const fn marker_delivery_seq(self) -> DeliverySeq {
        self.marker_delivery_seq
    }

    /// Returns the old binding epoch fenced by recovery.
    #[must_use]
    pub const fn recovered_binding_epoch(self) -> BindingEpoch {
        self.recovered_binding_epoch
    }
}

/// Public persisted input for transaction-order-frontier restoration.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OrderClaimFrontierRestore {
    /// Exact movable `A` and `X` claims.
    pub movable_claims: Vec<MovableOrderClaim>,
    /// Immutable candidate-major prefix above the caller-major high watermark.
    pub immutable_candidates: Vec<ImmutableOrderCandidateMajorRestore>,
    /// The sole optional DCR order interval.
    pub recovery: Option<RecoveryOrderBlockRestore>,
}

/// Validated exact transaction-order claim frontier.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderClaimFrontier {
    ledger: OrderLedger,
    movable_claims: Vec<MovableOrderClaim>,
    immutable_candidates: Vec<ImmutableOrderCandidateMajor>,
    recovery: Option<RecoveryOrderBlock>,
}

impl OrderClaimFrontier {
    /// Returns the aggregate ledger validated against these exact owners.
    #[must_use]
    pub const fn ledger(&self) -> OrderLedger {
        self.ledger
    }

    /// Borrows exact movable `A` and `X` claims.
    #[must_use]
    pub fn movable_claims(&self) -> &[MovableOrderClaim] {
        &self.movable_claims
    }

    /// Borrows immutable candidate-major groups in numeric order.
    #[must_use]
    pub fn immutable_candidates(&self) -> &[ImmutableOrderCandidateMajor] {
        &self.immutable_candidates
    }

    /// Returns the sole validated DCR order interval.
    #[must_use]
    pub const fn recovery(&self) -> Option<RecoveryOrderBlock> {
        self.recovery
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RecoveryClaimPhase {
    PreFate,
    PostFate,
    RecoveredBound,
}

/// Sealed recovery-claim provenance derived from one exact stored edge.
///
/// Before fate, only a marker-backed cursor witness can prove the full
/// `[T,RS,RT]` / `[A,RO,RA]` blocks. After fate, only the resulting
/// [`super::DetachedCredentialRecovery`] can prove the remaining pairs. The
/// prospective replacement epoch is deliberately absent: it does not exist
/// until fenced attach transfers `RT`/`RA` into `T`/`A`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveryClaimProvenance {
    participant_index: ParticipantId,
    marker_delivery_seq: DeliverySeq,
    prior_binding_epoch: BindingEpoch,
    current_binding_epoch: BindingEpoch,
    phase: RecoveryClaimPhase,
}

impl RecoveryClaimProvenance {
    /// Derives recovery authority only from a marker-backed PCP or the exact DCR
    /// edge produced by its binding fate.
    #[must_use]
    pub const fn from_stored_edge(edge: super::StoredEdge) -> Option<Self> {
        match edge {
            super::StoredEdge::MarkerDelivery(delivery) => Some(Self {
                participant_index: delivery.participant_id(),
                marker_delivery_seq: delivery.marker_delivery_seq(),
                prior_binding_epoch: delivery.binding_epoch(),
                current_binding_epoch: delivery.binding_epoch(),
                phase: RecoveryClaimPhase::PreFate,
            }),
            super::StoredEdge::ParticipantCursorProgress(progress) => {
                let Some(marker_delivery_seq) = progress.marker_delivery_seq() else {
                    return None;
                };
                Some(Self {
                    participant_index: progress.participant_id(),
                    marker_delivery_seq,
                    prior_binding_epoch: progress.binding_epoch(),
                    current_binding_epoch: progress.binding_epoch(),
                    phase: RecoveryClaimPhase::PreFate,
                })
            }
            super::StoredEdge::DetachedCredentialRecovery(recovery) => Some(Self {
                participant_index: recovery.participant_id(),
                marker_delivery_seq: recovery.marker_delivery_seq(),
                prior_binding_epoch: recovery.prior_binding_epoch(),
                current_binding_epoch: recovery.prior_binding_epoch(),
                phase: RecoveryClaimPhase::PostFate,
            }),
            _ => None,
        }
    }

    /// Returns the recovery participant.
    #[must_use]
    pub const fn participant_index(self) -> ParticipantId {
        self.participant_index
    }

    /// Returns the delivered marker that makes fenced recovery possible.
    #[must_use]
    pub const fn marker_delivery_seq(self) -> DeliverySeq {
        self.marker_delivery_seq
    }

    /// Returns the exact prior binding epoch.
    #[must_use]
    pub const fn prior_binding_epoch(self) -> BindingEpoch {
        self.prior_binding_epoch
    }
}

/// Public persisted input for restoring both coupled claim frontiers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimFrontiersRestore {
    /// Owning conversation for every typed identity/history authority.
    pub conversation_id: ConversationId,
    /// Raw current live identities; validation occurs after numeric frontiers.
    pub active_identities: Vec<FrontierParticipant>,
    /// Signed permanent identity-slot cap `I`.
    pub identity_slot_limit: u64,
    /// Current physical retained suffix floor.
    pub retained_floor: u128,
    /// Signed cap on retained record facts supplied by storage.
    pub retained_record_limit: u64,
    /// Typed retained direct-record facts used by provenance validation.
    pub retained_records: Vec<RetainedCausalRecord>,
    /// Retained marker sequences that still own current credit/anchor state.
    ///
    /// Released historical marker records remain in `retained_records` but are
    /// deliberately absent here. This bounded subset has at most one current
    /// owner per permanent participant and at most `I` entries.
    pub active_marker_anchors: Vec<DeliverySeq>,
    /// Immutable exact-epoch delivery facts for retained marker history.
    ///
    /// Unlike `active_marker_anchors`, this list is bounded by retained history
    /// rather than identity count and may contain multiple facts for one
    /// participant.
    pub historical_marker_deliveries: Vec<HistoricalMarkerDeliveryFactRestore>,
    /// O(I) factual compacted terminal/exit rows retained by identities and tombstones.
    pub historical_causal_facts: Vec<HistoricalCausalFactRestore>,
    /// Sequence-side persisted ownership.
    pub sequence: SequenceClaimFrontierRestore,
    /// Order-side persisted ownership.
    pub order: OrderClaimFrontierRestore,
    /// Exact planned or retained marker selected by the sole recovery quartet.
    ///
    /// This raw selector carries no participant or epoch authority. Restoration
    /// derives both solely from one fully validated marker candidate/record and,
    /// after binding fate, the exact typed DCR edge.
    pub recovery_marker_delivery_seq: Option<DeliverySeq>,
}

/// Numerically and causally prevalidated claim-frontier snapshot.
///
/// This crate-private phase breaks the cold-restore cycle without exposing a
/// forgeable marker token: retained history is validated first, storage uses at
/// most one sealed marker record to rebuild its typed edge, and only then may
/// recovery blocks be finalized against that edge.
#[derive(Debug)]
pub(super) struct ClaimFrontiersPrevalidated {
    conversation_id: ConversationId,
    active_identities: ActiveIdentityRanks,
    identity_slot_limit: u64,
    retained_floor: u128,
    retained_records: Vec<RetainedCausalRecord>,
    marker_records: Vec<RetainedCausalRecord>,
    historical_marker_deliveries: Vec<HistoricalMarkerDeliveryAuthority>,
    historical_causal_authorities: Vec<HistoricalCausalAuthority>,
    binding_origins: Vec<BindingOrigin>,
    sequence_restore: SequenceClaimFrontierRestore,
    order_restore: OrderClaimFrontierRestore,
    recovery_marker_delivery_seq: Option<DeliverySeq>,
    sequence_ledger: SequenceLedger,
    order_ledger: OrderLedger,
    issued_marker_record: Option<MarkerRecordRequest>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MarkerRecordUse {
    Planned(FrontierBinding),
    Delivered(FrontierBinding),
    Recovered {
        prior_binding_epoch: BindingEpoch,
        recovered_binding_epoch: BindingEpoch,
    },
}

/// Exact closure-derived context requesting one retained-marker restore token.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct MarkerRecordRequest {
    participant_index: ParticipantId,
    marker_delivery_seq: DeliverySeq,
    use_kind: MarkerRecordUse,
}

impl MarkerRecordRequest {
    /// Requests an undelivered planned marker in its exact current target state.
    pub(super) const fn planned(
        participant_index: ParticipantId,
        marker_delivery_seq: DeliverySeq,
        target: FrontierBinding,
    ) -> Self {
        Self {
            participant_index,
            marker_delivery_seq,
            use_kind: MarkerRecordUse::Planned(target),
        }
    }

    /// Requests a durably delivered marker in its exact current target state.
    pub(super) const fn delivered(
        participant_index: ParticipantId,
        marker_delivery_seq: DeliverySeq,
        target: FrontierBinding,
    ) -> Self {
        Self {
            participant_index,
            marker_delivery_seq,
            use_kind: MarkerRecordUse::Delivered(target),
        }
    }

    /// Requests the detached old-epoch predecessor of fenced recovery.
    pub(super) const fn recovered(
        participant_index: ParticipantId,
        marker_delivery_seq: DeliverySeq,
        prior_binding_epoch: BindingEpoch,
        recovered_binding_epoch: BindingEpoch,
    ) -> Self {
        Self {
            participant_index,
            marker_delivery_seq,
            use_kind: MarkerRecordUse::Recovered {
                prior_binding_epoch,
                recovered_binding_epoch,
            },
        }
    }
}

/// Crate-internal result of consuming the exact next marker candidate.
#[derive(Debug)]
pub(super) struct MarkerDrainCore {
    frontiers: ClaimFrontiers,
    candidate: ValidatedMarkerCandidate,
    record: ValidatedMarkerRecord,
}

impl MarkerDrainCore {
    /// Splits the indivisible frontier update from its fresh-edge and retained
    /// record authorities for the public marker-drain operation wrapper.
    pub(super) fn into_parts(
        self,
    ) -> (
        ClaimFrontiers,
        ValidatedMarkerCandidate,
        ValidatedMarkerRecord,
    ) {
        (self.frontiers, self.candidate, self.record)
    }
}

/// Invalid or non-marker mandatory prefix encountered by marker drain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MarkerDrainCoreError {
    /// No immutable candidate is currently owed.
    NoCandidate,
    /// A binding terminal has global precedence over marker work.
    BindingTerminalFirst,
    /// The first marker does not own exactly `H+1`.
    SequenceNotNext,
    /// A marker may share an already allocated causal major but cannot allocate one.
    CausalMajorNotAllocated,
    /// Cross-counter validation promised an order key that is now absent.
    MissingOrderCandidate,
    /// Consuming `M` did not yield a valid post-append sequence ledger.
    ResultingLedger,
}

/// Validated participant-keyed sequence and order claim authority.
#[derive(Debug, PartialEq, Eq)]
pub struct ClaimFrontiers {
    conversation_id: ConversationId,
    active_identities: ActiveIdentityRanks,
    identity_slot_limit: u64,
    retained_floor: u128,
    retained_records: Vec<RetainedCausalRecord>,
    marker_records: Vec<RetainedCausalRecord>,
    sequence: SequenceClaimFrontier,
    order: OrderClaimFrontier,
}

/// Atomic protocol-owned initial-enrollment frontier result.
///
/// The wrapper owns the typed operation commit together with the exact frontier,
/// closure accounting, and retained `Attached` charge derived from it. It is not
/// cloneable and exposes no raw restore components or caller-selected positions.
#[derive(Debug, PartialEq, Eq)]
pub struct InitialEnrollmentFrontierCommit<F> {
    operation: InitialEnrollmentOperationCommit<F>,
    frontiers: ClaimFrontiers,
    closure_accounting: ClosureAccounting,
    attached_charge: ResourceVector,
}

impl<F> InitialEnrollmentFrontierCommit<F> {
    /// Borrows the complete admitted enrollment operation.
    #[must_use]
    pub const fn operation(&self) -> &InitialEnrollmentOperationCommit<F> {
        &self.operation
    }

    /// Borrows the directly constructed coupled claim frontiers.
    #[must_use]
    pub const fn frontiers(&self) -> &ClaimFrontiers {
        &self.frontiers
    }

    /// Returns the exact closure accounting committed with the frontier.
    #[must_use]
    pub const fn closure_accounting(&self) -> ClosureAccounting {
        self.closure_accounting
    }

    /// Returns the exact encoded charge of the retained `Attached` row.
    #[must_use]
    pub const fn attached_charge(&self) -> ResourceVector {
        self.attached_charge
    }

    /// Consumes the atomic result for the crate-owned conversation event layer.
    #[allow(
        dead_code,
        reason = "the next conversation event body consumes this sealed operation/frontier unit"
    )]
    pub(in crate::lifecycle) fn into_conversation_parts(
        self,
    ) -> (
        InitialEnrollmentOperationCommit<F>,
        ClaimFrontiers,
        ClosureAccounting,
        ResourceVector,
    ) {
        (
            self.operation,
            self.frontiers,
            self.closure_accounting,
            self.attached_charge,
        )
    }
}

/// An admitted initial enrollment disagreed with its typed frontier projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InitialEnrollmentFrontierError {
    /// Supplied encoded `Attached` charge differs from the admitted projection.
    AttachedChargeMismatch {
        /// Charge fixed by the admitted closure projection.
        expected: ResourceVector,
        /// Charge supplied for the exact encoded `Attached` row.
        actual: ResourceVector,
    },
    /// Membership, binding, and `Attached` facts do not describe participant zero.
    EnrollmentShape,
    /// Operation record positions and aggregate ledgers disagree.
    LedgerShape,
    /// Floor, observer, marker, recovery, or closure facts disagree.
    ClosureProjection,
    /// Deriving exact direct/product positions overflowed their fixed-width domain.
    PositionOverflow,
    /// The directly constructed sequence and order owners failed cross-validation.
    FrontierInvariant,
}

/// Failed initial-frontier derivation retaining the speculative operation.
#[derive(Debug, PartialEq, Eq)]
pub struct InitialEnrollmentFrontierFailure<F> {
    operation: InitialEnrollmentOperationCommit<F>,
    error: InitialEnrollmentFrontierError,
}

impl<F> InitialEnrollmentFrontierFailure<F> {
    /// Returns the exact derivation fault.
    #[must_use]
    pub const fn error(&self) -> InitialEnrollmentFrontierError {
        self.error
    }

    /// Recovers the speculative operation for the crate-owned conversation layer.
    #[allow(
        dead_code,
        reason = "the conversation decision layer recovers or terminalizes the speculative enrollment"
    )]
    pub(in crate::lifecycle) fn into_operation(self) -> InitialEnrollmentOperationCommit<F> {
        self.operation
    }
}

/// Failure to consume the exact order authority for a Leave transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrepareLeaveAuthorityError {
    /// Member and frontier name different conversations.
    Conversation,
    /// Member identity/cursor is absent or disagrees with the frontier.
    Identity,
    /// Binding state disagrees with the validated identity frontier.
    Binding,
    /// A globally earlier immutable candidate must drain first.
    ImmutablePrefix,
    /// The exact pending binding-terminal candidate is absent or not sole.
    PendingCandidate,
    /// The participant's unique `X` order handle is absent.
    MembershipExitClaim,
    /// Bound Leave lacks its exact active-binding `A` handle.
    ActiveBindingClaim,
    /// The selected later handle cannot relay every survivor into the suffix.
    OrderCapacity,
    /// Consuming the selected handles could not produce a valid order ledger.
    ResultingOrderLedger,
}

/// Sealed marker candidate that passed complete frontier restoration.
///
/// This is the only authority from which the lifecycle module may construct a
/// new executable marker-delivery edge. Its private field prevents a storage
/// binding from turning raw participant/epoch/sequence values into that edge.
#[derive(Debug, PartialEq, Eq)]
pub struct ValidatedMarkerCandidate {
    conversation_id: ConversationId,
    candidate: MarkerCandidateAuthority,
    seal: MarkerAuthoritySeal,
}

/// Sealed retained marker record paired with its exact current authority.
///
/// Cold restoration of marker-derived edges consumes this token so raw storage
/// fields cannot fabricate durable marker delivery.
#[derive(Debug, PartialEq, Eq)]
pub struct ValidatedMarkerRecord {
    conversation_id: ConversationId,
    record: RetainedCausalRecord,
    provenance: MarkerProvenance,
    target_binding: FrontierBinding,
    occurrence: MarkerRecordOccurrence,
    seal: MarkerAuthoritySeal,
}

/// Delivery-occurrence state proven together with one retained marker record.
///
/// This discriminator is deliberately private. A retained append record does
/// not by itself prove that the marker reached its target binding; only joint
/// frontier restoration may upgrade an undelivered record to `Delivered` from
/// the exact historical delivery fact.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MarkerRecordOccurrence {
    /// The marker was appended but has not been delivered.
    Undelivered,
    /// The marker was durably delivered to the exact target binding.
    Delivered,
}

#[derive(Debug, PartialEq, Eq)]
enum MarkerAuthoritySeal {
    Validated,
}

impl ValidatedMarkerRecord {
    /// Consumes the one-shot retained-record token after a restore attempt.
    pub(super) const fn consume(self) {
        match self.seal {
            MarkerAuthoritySeal::Validated => {}
        }
    }

    /// Returns the owning conversation selected by complete frontier restore.
    #[must_use]
    pub const fn conversation_id(&self) -> ConversationId {
        self.conversation_id
    }

    /// Returns the permanent marker owner.
    #[must_use]
    pub const fn participant_id(&self) -> ParticipantId {
        self.record.admission_order.participant_index()
    }

    /// Returns the durable appended marker sequence.
    #[must_use]
    pub const fn delivery_seq(&self) -> DeliverySeq {
        self.record.delivery_seq
    }

    /// Returns the immutable retained-row causal key.
    #[must_use]
    pub const fn admission_order(&self) -> super::AdmissionOrder {
        self.record.admission_order
    }

    /// Returns immutable marker provenance.
    #[must_use]
    pub const fn provenance(&self) -> MarkerProvenance {
        self.provenance
    }

    /// Returns the current or last authoritative target binding.
    #[must_use]
    pub const fn target_binding(&self) -> FrontierBinding {
        self.target_binding
    }

    /// Returns the exact current or last authoritative epoch.
    #[must_use]
    pub const fn binding_epoch(&self) -> BindingEpoch {
        binding_epoch(self.target_binding)
    }

    /// Reports whether joint restoration proved the delivery occurrence.
    #[must_use]
    pub(super) const fn occurrence(&self) -> MarkerRecordOccurrence {
        self.occurrence
    }

    /// Marks a synthetic crate-test token as delivered.
    #[cfg(test)]
    pub(super) const fn delivered_for_test(mut self) -> Self {
        self.occurrence = MarkerRecordOccurrence::Delivered;
        self
    }
}

impl ValidatedMarkerCandidate {
    /// Consumes the one-shot fresh-candidate token after delivery materializes.
    pub(super) const fn consume(self) {
        match self.seal {
            MarkerAuthoritySeal::Validated => {}
        }
    }

    /// Returns the owning conversation selected by complete frontier restore.
    #[must_use]
    pub(super) const fn conversation_id(&self) -> ConversationId {
        self.conversation_id
    }

    /// Returns the permanent marker owner.
    #[must_use]
    pub(super) const fn participant_index(&self) -> ParticipantId {
        self.candidate.admission_order.participant_index()
    }

    /// Returns the permanent marker owner.
    #[must_use]
    pub(super) const fn participant_id(&self) -> ParticipantId {
        self.participant_index()
    }

    /// Returns the exact assigned marker sequence.
    #[must_use]
    pub(super) const fn delivery_seq(&self) -> DeliverySeq {
        self.candidate.delivery_seq
    }

    /// Returns the exact current or last authoritative target binding.
    #[must_use]
    pub(super) const fn target_binding(&self) -> FrontierBinding {
        self.candidate.target_binding
    }

    /// Returns immutable marker provenance.
    #[must_use]
    pub(super) const fn provenance(&self) -> MarkerProvenance {
        self.candidate.provenance
    }

    pub(super) const fn abandoned_after(&self) -> DeliverySeq {
        self.candidate.abandoned_after
    }

    pub(super) const fn abandoned_through(&self) -> DeliverySeq {
        self.candidate.abandoned_through
    }

    pub(super) const fn physical_floor_at_decision(&self) -> DeliverySeq {
        self.candidate.physical_floor_at_decision
    }
}

#[derive(Clone, Copy)]
struct InitialEnrollmentFrontierShape {
    conversation_id: ConversationId,
    binding_epoch: BindingEpoch,
    identity_slot_limit: u64,
    retained_floor: u128,
    attached: AttachedLifecycleRecord,
    order_ledger: OrderLedger,
    sequence_ledger: SequenceLedger,
    closure_accounting: ClosureAccounting,
}

#[derive(Clone, Copy)]
struct InitialFrontierPositions {
    terminal_sequence: DeliverySeq,
    exit_sequence: DeliverySeq,
    product_sequence: DeliverySeq,
    active_terminal_order: TransactionOrder,
    exit_order: TransactionOrder,
}

fn initial_enrollment_frontier_shape<F>(
    operation: &InitialEnrollmentOperationCommit<F>,
    attached_charge: ResourceVector,
) -> Result<InitialEnrollmentFrontierShape, InitialEnrollmentFrontierError> {
    let projection = operation.closure_projection();
    let expected_charge = projection.resulting_retained_charge();
    if attached_charge != expected_charge {
        return Err(InitialEnrollmentFrontierError::AttachedChargeMismatch {
            expected: expected_charge,
            actual: attached_charge,
        });
    }
    let enrollment = operation.enrollment();
    let attached = enrollment.attached;
    let BindingState::Bound(binding) = enrollment.binding_state else {
        return Err(InitialEnrollmentFrontierError::EnrollmentShape);
    };
    if enrollment.member.participant_id() != 0
        || enrollment.member.cursor() != 0
        || binding.participant_id != 0
        || attached.participant_id() != 0
        || binding.conversation_id != enrollment.member.conversation_id()
        || attached.conversation_id() != binding.conversation_id
        || attached.binding_epoch() != binding.binding_epoch
        || projection.participant_index() != 0
        || projection.binding_epoch() != binding.binding_epoch
        || projection.identity_slots() == 0
    {
        return Err(InitialEnrollmentFrontierError::EnrollmentShape);
    }
    let admission_order = attached.admission_order();
    let order_ledger = operation.order().resulting();
    let sequence_ledger = operation.sequence().resulting();
    let order_claims = order_ledger.claims();
    let sequence_claims = sequence_ledger.claims();
    if operation.order().major() != admission_order.transaction_order()
        || !matches!(order_ledger.high(), OrderHigh::Allocated(value) if value == admission_order.transaction_order())
        || order_claims.active_binding_terminals() != 1
        || order_claims.membership_exits() != 1
        || order_claims.recovery_operation()
        || order_claims.recovery_replacement_terminal()
        || sequence_ledger.high_watermark() != attached.delivery_seq()
        || sequence_claims.live_members() != 1
        || sequence_claims.binding_terminals() != 1
        || sequence_claims.markers() != 0
        || sequence_claims.recovery() != RecoverySequenceReserve::None
        || admission_order.candidate_phase() != CandidatePhase::AttachLifecycle
    {
        return Err(InitialEnrollmentFrontierError::LedgerShape);
    }
    Ok(InitialEnrollmentFrontierShape {
        conversation_id: binding.conversation_id,
        binding_epoch: binding.binding_epoch,
        identity_slot_limit: projection.identity_slots(),
        retained_floor: projection.resulting_floor(),
        attached,
        order_ledger,
        sequence_ledger,
        closure_accounting: validate_initial_enrollment_closure(operation, projection, attached)?,
    })
}

fn validate_initial_enrollment_closure<F>(
    operation: &InitialEnrollmentOperationCommit<F>,
    projection: &InitialEnrollmentClosureProjection,
    attached: AttachedLifecycleRecord,
) -> Result<ClosureAccounting, InitialEnrollmentFrontierError> {
    let accounting = projection.resulting_closure_accounting();
    let state_matches = match accounting.state() {
        ClosureState::Clear => {
            projection.debt().is_zero()
                && projection.remaining_recovery_claim() == ResourceVector::default()
        }
        ClosureState::Owed {
            debt,
            edge: StoredEdge::ObserverProjection(observer),
        } => {
            debt.value() == projection.debt()
                && observer == ObserverProjection::new(attached.delivery_seq())
                && projection.remaining_recovery_claim() == accounting.edge_k_remaining()
        }
        ClosureState::Owed { .. } => false,
    };
    if projection.resulting_floor() != 1
        || projection.resulting_floor() != operation.observer_floor().cap_floor()
        || operation.observer_floor().observer_progress() != 0
        || projection.recovery_quartet() != RecoveryQuartetStatus::None
        || !projection.new_marker_candidates().is_empty()
        || accounting.marker_capacity_credits() != 0
        || accounting.marker_anchors() != 0
        || accounting.edge_sequence_claims() != 0
        || accounting.edge_order_position_claims() != 0
        || accounting.baseline() != projection.resulting_baseline()
        || !state_matches
    {
        Err(InitialEnrollmentFrontierError::ClosureProjection)
    } else {
        Ok(accounting)
    }
}

fn initial_frontier_positions(
    attached: AttachedLifecycleRecord,
) -> Result<InitialFrontierPositions, InitialEnrollmentFrontierError> {
    let terminal_sequence = attached
        .delivery_seq()
        .checked_add(1)
        .ok_or(InitialEnrollmentFrontierError::PositionOverflow)?;
    let exit_sequence = terminal_sequence
        .checked_add(1)
        .ok_or(InitialEnrollmentFrontierError::PositionOverflow)?;
    let product_sequence = exit_sequence
        .checked_add(1)
        .ok_or(InitialEnrollmentFrontierError::PositionOverflow)?;
    let active_terminal_order = attached
        .admission_order()
        .transaction_order()
        .checked_add(1)
        .ok_or(InitialEnrollmentFrontierError::PositionOverflow)?;
    let exit_order = active_terminal_order
        .checked_add(1)
        .ok_or(InitialEnrollmentFrontierError::PositionOverflow)?;
    Ok(InitialFrontierPositions {
        terminal_sequence,
        exit_sequence,
        product_sequence,
        active_terminal_order,
        exit_order,
    })
}

fn initial_sequence_frontier(
    shape: &InitialEnrollmentFrontierShape,
    positions: InitialFrontierPositions,
    terminal: BindingTerminalOwner,
) -> SequenceClaimFrontier {
    SequenceClaimFrontier {
        ledger: shape.sequence_ledger,
        movable_claims: alloc::vec![
            MovableSequenceClaim {
                delivery_seq: positions.terminal_sequence,
                owner: SequenceDirectOwner::BindingTerminal(terminal),
            },
            MovableSequenceClaim {
                delivery_seq: positions.exit_sequence,
                owner: SequenceDirectOwner::MembershipExit {
                    participant_index: 0,
                },
            },
        ],
        immutable_candidates: Vec::new(),
        products: SequenceProductRanges {
            live_times_terminal: alloc::vec![TerminalProductRange {
                start: positions.product_sequence,
                length: 1,
                terminal,
            }],
            live_times_replacement_terminal: None,
            other_live_times_exit: Vec::new(),
        },
        recovery: None,
    }
}

fn initial_order_frontier(
    shape: &InitialEnrollmentFrontierShape,
    positions: InitialFrontierPositions,
    terminal: BindingTerminalOwner,
) -> OrderClaimFrontier {
    OrderClaimFrontier {
        ledger: shape.order_ledger,
        movable_claims: alloc::vec![
            MovableOrderClaim {
                transaction_order: positions.active_terminal_order,
                owner: OrderDirectOwner::ActiveBindingTerminal(terminal),
            },
            MovableOrderClaim {
                transaction_order: positions.exit_order,
                owner: OrderDirectOwner::MembershipExit {
                    participant_index: 0,
                },
            },
        ],
        immutable_candidates: Vec::new(),
        recovery: None,
    }
}

fn build_initial_enrollment_frontiers(
    shape: &InitialEnrollmentFrontierShape,
) -> Result<ClaimFrontiers, InitialEnrollmentFrontierError> {
    let positions = initial_frontier_positions(shape.attached)?;
    let terminal = BindingTerminalOwner {
        participant_index: 0,
        binding_epoch: shape.binding_epoch,
    };
    let sequence = initial_sequence_frontier(shape, positions, terminal);
    let order = initial_order_frontier(shape, positions, terminal);
    validate_cross_counter(&sequence, &order)
        .map_err(|_| InitialEnrollmentFrontierError::FrontierInvariant)?;
    Ok(ClaimFrontiers {
        conversation_id: shape.conversation_id,
        active_identities: ActiveIdentityRanks {
            participants: alloc::vec![FrontierParticipant::new(
                0,
                0,
                FrontierBinding::Bound(shape.binding_epoch),
            )],
        },
        identity_slot_limit: shape.identity_slot_limit,
        retained_floor: shape.retained_floor,
        retained_records: alloc::vec![RetainedCausalRecord {
            delivery_seq: shape.attached.delivery_seq(),
            admission_order: shape.attached.admission_order(),
            kind: RetainedCausalRecordKind::AttachLifecycle {
                participant_index: 0,
                binding_epoch: shape.binding_epoch,
            },
        }],
        marker_records: Vec::new(),
        sequence,
        order,
    })
}

#[derive(Clone, Copy)]
enum LeaveSequenceUnit {
    Direct(MovableSequenceClaim),
    TerminalProduct(TerminalProductRange),
    ReplacementProduct(ReplacementTerminalProductRange),
    ExitProduct(ExitProductRange),
    Recovery(RecoverySequenceBlock),
}

impl LeaveSequenceUnit {
    fn original_start(self) -> DeliverySeq {
        match self {
            Self::Direct(claim) => claim.delivery_seq,
            Self::TerminalProduct(range) => range.start,
            Self::ReplacementProduct(range) => range.start,
            Self::ExitProduct(range) => range.start,
            Self::Recovery(block) => block_start_validated_sequence(block),
        }
    }
}

fn allocate_leave_sequence_range(
    cursor: &mut Option<DeliverySeq>,
    length: u64,
) -> Result<DeliverySeq, LeaveCommitError> {
    let Some(start) = *cursor else {
        return Err(LeaveCommitError::ResultingFrontier);
    };
    let end = u128::from(start)
        .checked_add(u128::from(length))
        .and_then(|value| value.checked_sub(1))
        .ok_or(LeaveCommitError::ResultingFrontier)?;
    if end > u128::from(u64::MAX) {
        return Err(LeaveCommitError::ResultingFrontier);
    }
    *cursor = u64::try_from(end + 1).ok();
    Ok(start)
}

/// Protocol-internal failure while deriving a live frontier from a typed lifecycle commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::lifecycle) enum LiveFrontierTransitionError {
    /// The typed commit names another conversation or participant history.
    Authority,
    /// An immutable candidate or recovery interval must be handled by its dedicated transition.
    Precedence,
    /// The commit's retained rows do not immediately follow the current durable high watermark.
    RecordPosition,
    /// Checked claim relocation exceeded the fixed-width sequence or order domain.
    Exhausted,
    /// Derived exact owners disagree with the protocol-produced aggregate ledgers.
    ResultingFrontier,
}

fn select_retained_marker_records(records: &[RetainedCausalRecord]) -> Vec<RetainedCausalRecord> {
    records
        .iter()
        .copied()
        .filter(|record| {
            matches!(
                record.kind,
                RetainedCausalRecordKind::CompactionMarker { .. }
            )
        })
        .collect()
}

impl ClaimFrontiers {
    /// Constructs the complete initial frontier directly from one admitted
    /// enrollment operation and its exact encoded `Attached` charge.
    ///
    /// No restore representation, row list, claim list, or numeric position is
    /// accepted from the caller. Participant zero, the retained lifecycle row,
    /// `A`/`X`, `T`/`E`, and `L x T` owners are derived solely from the opaque
    /// operation commit after its closure projection and aggregate ledgers are
    /// cross-checked.
    ///
    /// # Errors
    ///
    /// Returns [`InitialEnrollmentFrontierError`] when the supplied charge or
    /// any typed operation/projection invariant disagrees with the canonical
    /// initial frontier.
    pub fn from_initial_enrollment<F>(
        operation: InitialEnrollmentOperationCommit<F>,
        attached_charge: ResourceVector,
    ) -> Result<InitialEnrollmentFrontierCommit<F>, Box<InitialEnrollmentFrontierFailure<F>>> {
        let shape = match initial_enrollment_frontier_shape(&operation, attached_charge) {
            Ok(shape) => shape,
            Err(error) => {
                return Err(Box::new(InitialEnrollmentFrontierFailure {
                    operation,
                    error,
                }));
            }
        };
        let closure_accounting = shape.closure_accounting;
        let frontiers = match build_initial_enrollment_frontiers(&shape) {
            Ok(frontiers) => frontiers,
            Err(error) => {
                return Err(Box::new(InitialEnrollmentFrontierFailure {
                    operation,
                    error,
                }));
            }
        };
        Ok(InitialEnrollmentFrontierCommit {
            operation,
            frontiers,
            closure_accounting,
            attached_charge,
        })
    }

    /// Restores exact frontiers only when their numeric unions, logical owners,
    /// product descriptors, DCR intervals, candidate keys, and aggregate ledgers
    /// all agree.
    ///
    /// This standalone form accepts no compacted causal-history rows or binding
    /// origins. Snapshots containing either must use the protocol-owned event
    /// replay path so participant-owned history is restored first.
    ///
    /// # Errors
    ///
    /// Returns the deterministic first delivery-sequence fault before checking
    /// transaction order, then checks cross-counter candidate and DCR identity.
    pub fn restore(
        restore: ClaimFrontiersRestore,
        sequence_ledger: SequenceLedger,
        order_ledger: OrderLedger,
    ) -> Result<Self, ParticipantStateCorruptReason> {
        let history = ValidatedConversationHistory::empty();
        Self::prevalidate_with_history(restore, sequence_ledger, order_ledger, &history)?
            .finish(None)
    }

    /// Validates numeric frontiers and durable marker history before a stored
    /// edge is reconstructed from the resulting sealed authority.
    #[cfg(test)]
    pub(super) fn prevalidate(
        restore: ClaimFrontiersRestore,
        sequence_ledger: SequenceLedger,
        order_ledger: OrderLedger,
    ) -> Result<ClaimFrontiersPrevalidated, ParticipantStateCorruptReason> {
        let history = ValidatedConversationHistory::empty();
        Self::prevalidate_with_history(restore, sequence_ledger, order_ledger, &history)
    }

    pub(super) fn prevalidate_with_history(
        restore: ClaimFrontiersRestore,
        sequence_ledger: SequenceLedger,
        order_ledger: OrderLedger,
        history: &ValidatedConversationHistory,
    ) -> Result<ClaimFrontiersPrevalidated, ParticipantStateCorruptReason> {
        validate_sequence_numeric(&restore.sequence, sequence_ledger).map_err(corrupt_frontier)?;
        validate_order_numeric(&restore.order, order_ledger).map_err(corrupt_frontier)?;
        validate_unique_candidate_keys(
            &restore.sequence.immutable_candidates,
            &restore.retained_records,
        )?;
        validate_bounded_shape(&restore, sequence_ledger).map_err(corrupt_frontier)?;
        let active_identities = ActiveIdentityRanks::try_new(
            restore.active_identities,
            sequence_ledger.high_watermark(),
            restore.identity_slot_limit,
        )
        .map_err(corrupt_frontier)?;
        let retained_records = validated_retained_records(
            restore.retained_records,
            restore.retained_floor,
            restore.retained_record_limit,
            restore.identity_slot_limit,
            sequence_ledger,
        )
        .map_err(corrupt_frontier)?;
        let historical_causal_authorities = validated_historical_authorities(
            restore.historical_causal_facts,
            restore.conversation_id,
            restore.identity_slot_limit,
            sequence_ledger,
            history,
        )
        .map_err(corrupt_frontier)?;
        let retained_marker_records = select_retained_marker_records(&retained_records);
        let marker_records = validated_active_marker_records(
            &retained_marker_records,
            restore.active_marker_anchors,
            restore.identity_slot_limit,
            sequence_ledger,
        )
        .map_err(corrupt_frontier)?;
        let historical_marker_deliveries = validated_historical_marker_deliveries(
            restore.historical_marker_deliveries,
            restore.conversation_id,
            &active_identities,
            &retained_records,
            &historical_causal_authorities,
            restore.retained_record_limit,
            sequence_ledger,
        )
        .map_err(corrupt_frontier)?;
        BindingOriginValidation {
            conversation_id: restore.conversation_id,
            active: &active_identities,
            origins: &history.binding_origins,
            retained_records: &retained_records,
            causal_authorities: &history.causal_authorities,
            historical_marker_deliveries: &historical_marker_deliveries,
            total: history.total,
            ledger: sequence_ledger,
        }
        .validate()
        .map_err(corrupt_frontier)?;
        validate_sequence_candidates(
            &active_identities,
            &restore.sequence.immutable_candidates,
            restore.retained_floor,
            &retained_records,
            &historical_causal_authorities,
            sequence_ledger,
        )
        .map_err(corrupt_frontier)?;
        validate_marker_credit_owners(
            &restore.sequence.immutable_candidates,
            &marker_records,
            restore.identity_slot_limit,
            sequence_ledger,
        )
        .map_err(corrupt_frontier)?;
        Ok(ClaimFrontiersPrevalidated {
            conversation_id: restore.conversation_id,
            active_identities,
            identity_slot_limit: restore.identity_slot_limit,
            retained_floor: restore.retained_floor,
            retained_records,
            marker_records,
            historical_marker_deliveries,
            historical_causal_authorities,
            binding_origins: history.binding_origins.clone(),
            sequence_restore: restore.sequence,
            order_restore: restore.order,
            recovery_marker_delivery_seq: restore.recovery_marker_delivery_seq,
            sequence_ledger,
            order_ledger,
            issued_marker_record: None,
        })
    }

    /// Borrows sorted current live identities.
    #[must_use]
    pub const fn active_identities(&self) -> &ActiveIdentityRanks {
        &self.active_identities
    }

    /// Returns the signed permanent identity-slot capacity validated at restore.
    #[must_use]
    pub const fn identity_slot_limit(&self) -> u64 {
        self.identity_slot_limit
    }

    /// Returns the owning conversation.
    #[must_use]
    pub const fn conversation_id(&self) -> ConversationId {
        self.conversation_id
    }

    /// Returns the current physical retained suffix floor.
    #[must_use]
    pub const fn retained_floor(&self) -> u128 {
        self.retained_floor
    }

    /// Borrows every validated physical row in the retained sequence suffix.
    #[must_use]
    pub fn retained_records(&self) -> &[RetainedCausalRecord] {
        &self.retained_records
    }

    /// Borrows only the O(I) retained marker anchors needed by executable edges.
    #[must_use]
    pub fn retained_marker_records(&self) -> &[RetainedCausalRecord] {
        &self.marker_records
    }

    /// Projects exact offered-marker cursor progress from one validated retained
    /// marker and its current bound identity.
    ///
    /// Raw participant, epoch, and sequence inputs grant no authority: all must
    /// match the coupled frontier's retained marker anchor and active binding.
    /// The returned progress is derived only through sealed [`MarkerDelivery`]
    /// authority and the exact delivered event.
    #[must_use]
    pub fn project_offered_marker_progress(
        &self,
        participant_id: ParticipantId,
        binding_epoch: BindingEpoch,
        marker_delivery_seq: DeliverySeq,
        event: Event,
    ) -> Option<ParticipantCursorProgress> {
        let record = self
            .marker_records
            .iter()
            .find(|record| record.delivery_seq == marker_delivery_seq)
            .copied()?;
        let RetainedCausalRecordKind::CompactionMarker {
            participant_index,
            provenance,
        } = record.kind
        else {
            return None;
        };
        if participant_index != participant_id {
            return None;
        }
        let participant = active_participant(&self.active_identities, participant_id)?;
        let target_binding = FrontierBinding::Bound(binding_epoch);
        if participant.binding != target_binding {
            return None;
        }
        let authority = ValidatedMarkerRecord {
            conversation_id: self.conversation_id,
            record,
            provenance,
            target_binding,
            occurrence: MarkerRecordOccurrence::Undelivered,
            seal: MarkerAuthoritySeal::Validated,
        };
        MarkerDelivery::from_validated_record(&authority)
            .delivered_progress(event)
            .ok()
    }

    /// Borrows the validated sequence frontier.
    #[must_use]
    pub const fn sequence(&self) -> &SequenceClaimFrontier {
        &self.sequence
    }

    /// Borrows the validated transaction-order frontier.
    #[must_use]
    pub const fn order(&self) -> &OrderClaimFrontier {
        &self.order
    }

    #[cfg(test)]
    pub(in crate::lifecycle) fn cross_counter_valid_for_test(&self) -> bool {
        validate_cross_counter(&self.sequence, &self.order).is_ok()
    }

    /// Applies one protocol-normalized live lifecycle transition.
    ///
    /// Only sibling lifecycle operations can call this seam. They derive the
    /// identities, rows, and aggregate ledgers from sealed typed commits; no
    /// storage or server caller can provide raw frontier components.
    pub(in crate::lifecycle) fn apply_live_transition(
        self,
        active_identities: Vec<FrontierParticipant>,
        appended_records: &[RetainedCausalRecord],
        sequence_ledger: SequenceLedger,
        order_ledger: OrderLedger,
    ) -> Result<Self, Box<(Self, LiveFrontierTransitionError)>> {
        if !self.sequence.immutable_candidates.is_empty()
            || self.sequence.recovery.is_some()
            || !self.order.immutable_candidates.is_empty()
            || self.order.recovery.is_some()
        {
            return Err(Box::new((self, LiveFrontierTransitionError::Precedence)));
        }
        let Ok(active) = ActiveIdentityRanks::try_new(
            active_identities,
            sequence_ledger.high_watermark(),
            self.identity_slot_limit,
        ) else {
            return Err(Box::new((self, LiveFrontierTransitionError::Authority)));
        };
        let first_sequence = self.sequence.ledger.high_watermark().checked_add(1);
        if first_sequence.is_none_or(|first| {
            appended_records.iter().enumerate().any(|(index, record)| {
                u64::try_from(index)
                    .ok()
                    .and_then(|offset| first.checked_add(offset))
                    != Some(record.delivery_seq)
            })
        }) || appended_records
            .last()
            .is_some_and(|record| record.delivery_seq != sequence_ledger.high_watermark())
        {
            return Err(Box::new((
                self,
                LiveFrontierTransitionError::RecordPosition,
            )));
        }
        let (sequence, order) =
            match rebuild_unreserved_frontiers(&active, sequence_ledger, order_ledger) {
                Ok(frontiers) => frontiers,
                Err(error) => return Err(Box::new((self, error))),
            };
        let Self {
            conversation_id,
            identity_slot_limit,
            retained_floor,
            mut retained_records,
            marker_records,
            ..
        } = self;
        retained_records.extend_from_slice(appended_records);
        Ok(Self {
            conversation_id,
            active_identities: active,
            identity_slot_limit,
            retained_floor,
            retained_records,
            marker_records,
            sequence,
            order,
        })
    }

    /// Consumes the exact coupled DCR blocks into one fenced attach.
    ///
    /// This seam is lifecycle-private: its participant and rows are derived from
    /// the sealed attach commit, never supplied by storage. Both recovery blocks,
    /// the delivered marker, the detached epoch, and every reserved position are
    /// checked before `RS`/`RO` are consumed and `RT`/`RA` become the recovered
    /// binding's ordinary `T`/`A` claims.
    pub(in crate::lifecycle) fn apply_live_fenced_attach(
        self,
        participant: FrontierParticipant,
        prior_binding_epoch: BindingEpoch,
        appended_records: &[RetainedCausalRecord],
    ) -> Result<Self, Box<(Self, LiveFrontierTransitionError)>> {
        let Some(current) = self
            .active_identities
            .participants()
            .iter()
            .find(|current| current.participant_index() == participant.participant_index())
            .copied()
        else {
            return Err(Box::new((self, LiveFrontierTransitionError::Authority)));
        };
        let Some(sequence_recovery) = self.sequence.recovery else {
            return Err(Box::new((self, LiveFrontierTransitionError::Precedence)));
        };
        let Some(order_recovery) = self.order.recovery else {
            return Err(Box::new((self, LiveFrontierTransitionError::Precedence)));
        };
        if !Self::fenced_recovery_authority_matches(
            current,
            participant,
            prior_binding_epoch,
            sequence_recovery,
            order_recovery,
        ) {
            return Err(Box::new((self, LiveFrontierTransitionError::Authority)));
        }
        if !self.sequence.immutable_candidates.is_empty()
            || !self.order.immutable_candidates.is_empty()
        {
            return Err(Box::new((self, LiveFrontierTransitionError::Precedence)));
        }
        if !Self::fenced_records_match(
            participant,
            sequence_recovery,
            order_recovery,
            appended_records,
        ) {
            return Err(Box::new((
                self,
                LiveFrontierTransitionError::RecordPosition,
            )));
        }
        if !self.has_recovery_marker(participant) {
            return Err(Box::new((self, LiveFrontierTransitionError::Authority)));
        }
        if self.has_duplicate_appended_record(appended_records) {
            return Err(Box::new((
                self,
                LiveFrontierTransitionError::RecordPosition,
            )));
        }
        let Ok(sequence_ledger) = self.sequence.ledger.apply_fenced_recovery() else {
            return Err(Box::new((
                self,
                LiveFrontierTransitionError::ResultingFrontier,
            )));
        };
        let Ok(order_ledger) = self.order.ledger.apply_fenced_recovery() else {
            return Err(Box::new((
                self,
                LiveFrontierTransitionError::ResultingFrontier,
            )));
        };
        let mut active = self.active_identities.participants().to_vec();
        let Some(current) = active
            .iter_mut()
            .find(|current| current.participant_index() == participant.participant_index())
        else {
            return Err(Box::new((self, LiveFrontierTransitionError::Authority)));
        };
        *current = participant;
        let Ok(active) = ActiveIdentityRanks::try_new(
            active,
            sequence_ledger.high_watermark(),
            self.identity_slot_limit,
        ) else {
            return Err(Box::new((self, LiveFrontierTransitionError::Authority)));
        };
        let Ok((sequence, order)) =
            rebuild_unreserved_frontiers(&active, sequence_ledger, order_ledger)
        else {
            return Err(Box::new((
                self,
                LiveFrontierTransitionError::ResultingFrontier,
            )));
        };
        let mut resulting = self;
        resulting.active_identities = active;
        resulting
            .retained_records
            .extend_from_slice(appended_records);
        resulting
            .retained_records
            .sort_unstable_by_key(|record| record.delivery_seq);
        resulting
            .marker_records
            .retain(|record| record.delivery_seq != participant.cursor());
        resulting.sequence = sequence;
        resulting.order = order;
        Ok(resulting)
    }

    fn fenced_recovery_authority_matches(
        current: FrontierParticipant,
        participant: FrontierParticipant,
        prior_binding_epoch: BindingEpoch,
        sequence_recovery: RecoverySequenceBlock,
        order_recovery: RecoveryOrderBlock,
    ) -> bool {
        let participant_matches = sequence_recovery.participant_index()
            == participant.participant_index()
            && order_recovery.participant_index() == participant.participant_index();
        let marker_matches = sequence_recovery.marker_delivery_seq() == participant.cursor()
            && order_recovery.marker_delivery_seq() == participant.cursor();
        let prior_epoch_matches = sequence_recovery.recovered_binding_epoch()
            == prior_binding_epoch
            && order_recovery.recovered_binding_epoch() == prior_binding_epoch;
        participant_matches
            && marker_matches
            && prior_epoch_matches
            && current.binding() == FrontierBinding::Detached(prior_binding_epoch)
            && current.cursor() <= participant.cursor()
            && matches!(participant.binding(), FrontierBinding::Bound(_))
    }

    fn fenced_records_match(
        participant: FrontierParticipant,
        sequence_recovery: RecoverySequenceBlock,
        order_recovery: RecoveryOrderBlock,
        appended_records: &[RetainedCausalRecord],
    ) -> bool {
        let Some(attached) = appended_records.last().copied() else {
            return false;
        };
        let FrontierBinding::Bound(recovered_binding_epoch) = participant.binding() else {
            return false;
        };
        let attached_matches = attached.delivery_seq == sequence_recovery.recovery_attach_seq()
            && attached.admission_order.transaction_order()
                == order_recovery.recovery_operation_order()
            && attached.kind
                == (RetainedCausalRecordKind::AttachLifecycle {
                    participant_index: participant.participant_index(),
                    binding_epoch: recovered_binding_epoch,
                });
        if !attached_matches {
            return false;
        }
        let prefix = &appended_records[..appended_records.len() - 1];
        match (
            sequence_recovery.terminal(),
            order_recovery.active_binding(),
            prefix,
        ) {
            (None, None, []) => true,
            (Some(sequence_terminal), Some(order_terminal), [terminal]) => {
                sequence_terminal.owner == order_terminal.owner
                    && terminal.delivery_seq == sequence_terminal.delivery_seq
                    && terminal.admission_order.transaction_order()
                        == order_terminal.transaction_order
                    && terminal.kind
                        == RetainedCausalRecordKind::BindingTerminal(sequence_terminal.owner)
            }
            _ => false,
        }
    }

    fn has_recovery_marker(&self, participant: FrontierParticipant) -> bool {
        self.retained_records.iter().any(|record| {
            record.delivery_seq == participant.cursor()
                && matches!(
                    record.kind,
                    RetainedCausalRecordKind::CompactionMarker { participant_index, .. }
                        if participant_index == participant.participant_index()
                )
        })
    }

    fn has_duplicate_appended_record(&self, appended_records: &[RetainedCausalRecord]) -> bool {
        appended_records.iter().any(|row| {
            self.retained_records
                .iter()
                .any(|retained| retained.delivery_seq == row.delivery_seq)
        })
    }

    /// Applies an acknowledgement's exact cursor/binding facts without exposing
    /// the participant vector as a server mutation API.
    pub(in crate::lifecycle) fn apply_live_identity(
        mut self,
        participant: FrontierParticipant,
    ) -> Result<Self, Box<(Self, LiveFrontierTransitionError)>> {
        let Some(current) = self
            .active_identities
            .participants
            .iter_mut()
            .find(|current| current.participant_index == participant.participant_index)
        else {
            return Err(Box::new((self, LiveFrontierTransitionError::Authority)));
        };
        if participant.cursor < current.cursor
            || participant.cursor > self.sequence.ledger.high_watermark()
        {
            return Err(Box::new((self, LiveFrontierTransitionError::Authority)));
        }
        *current = participant;
        Ok(self)
    }

    /// Consumes one complete validated frontier into the ordinary record fixed
    /// point, preventing storage callers from supplying disconnected retained
    /// rows, participant cursors, immutable candidates, or aggregate ledgers.
    ///
    /// Exact keyed row charges remain durability facts. They are joined to the
    /// owned rows here before any floor/capacity/counter transition executes.
    /// The returned decision owns either the unchanged prestate and its exact
    /// earlier candidate, or the complete projected poststate.
    ///
    /// # Errors
    ///
    /// Returns [`OrdinaryRecordProjectionFailure`] for a conversation/binding
    /// mismatch, malformed keyed charges/accounting, capacity or observer
    /// refusal, counter exhaustion, or an impossible exact-owner relocation.
    /// Every failure owns the unchanged frontier and original projection input.
    pub fn project_ordinary_record(
        self,
        input: OrdinaryRecordProjectionInput,
    ) -> Result<OrdinaryRecordProjectionDecision, Box<OrdinaryRecordProjectionFailure>> {
        let (
            request,
            receiving_binding_epoch,
            encoded_record_charge,
            retained_charges,
            observer_progress,
            closure_accounting,
            limits,
        ) = input.as_parts();
        if request.conversation_id != self.conversation_id {
            return Err(projection_failure(
                self,
                input,
                OrdinaryProjectionError::Conversation,
            ));
        }
        let unaccepted_marker_anchors = ordinary_unaccepted_marker_anchors(&self);
        let kernel = match project_ordinary_fixed_point(&OrdinaryProjectionFacts {
            request: request.clone(),
            receiving_binding_epoch,
            encoded_record_charge,
            retained_records: &self.retained_records,
            retained_charges,
            active_marker_credit_records: &self.marker_records,
            unaccepted_marker_anchors: &unaccepted_marker_anchors,
            active_identities: self.active_identities.participants(),
            identity_slot_limit: self.identity_slot_limit,
            current_floor: self.retained_floor,
            observer_progress,
            order_ledger: self.order.ledger,
            sequence_ledger: self.sequence.ledger,
            immutable_candidates: &self.sequence.immutable_candidates,
            closure_accounting,
            remaining_recovery_claim: closure_accounting.edge_k_remaining(),
            limits,
        }) {
            Ok(value) => value,
            Err(error) => return Err(projection_failure(self, input, error)),
        };
        match kernel {
            OrdinaryProjectionKernelDecision::DrainFirst(prefix) => Ok(
                OrdinaryRecordProjectionDecision::DrainFirst(Box::new(OrdinaryRecordDrainFirst {
                    frontiers: self,
                    input,
                    candidate: prefix.candidate(),
                })),
            ),
            OrdinaryProjectionKernelDecision::Projected(projected) => {
                let observer_floor = match check_observer_floor(
                    ObserverCheckedOperation::RecordAdmission(request.clone()),
                    observer_progress,
                    projected.floor().resulting_floor,
                ) {
                    ObserverFloorDecision::Eligible(permit) => permit,
                    ObserverFloorDecision::Respond(_) => {
                        return Err(projection_failure(
                            self,
                            input,
                            OrdinaryProjectionError::ObserverSelectorInvariant,
                        ));
                    }
                };
                let closure = match check_remaining_closure(
                    &ClosureCheckedEnvelope::RecordAdmission(request.clone()),
                    closure_accounting,
                    false,
                    0,
                    projected.required_capacity(),
                ) {
                    RemainingClosureDecision::Eligible(permit) => *permit,
                    RemainingClosureDecision::Respond(_) => {
                        return Err(projection_failure(
                            self,
                            input,
                            OrdinaryProjectionError::ClosureSelectorInvariant,
                        ));
                    }
                };
                match self.apply_ordinary_projection(*projected, observer_floor, closure) {
                    Ok(projected) => Ok(OrdinaryRecordProjectionDecision::Projected(Box::new(
                        projected,
                    ))),
                    Err(failure) => {
                        let (frontiers, error) = *failure;
                        Err(projection_failure(frontiers, input, error))
                    }
                }
            }
        }
    }

    fn apply_ordinary_projection(
        mut self,
        projected: OrdinaryFixedPointPlan,
        observer_floor: super::ObserverFloorPermit,
        closure: super::RemainingClosurePermit,
    ) -> Result<ProjectedOrdinaryRecord, Box<(Self, OrdinaryProjectionError)>> {
        let Ok(marker_count) = u64::try_from(projected.marker_candidates().len()) else {
            return Err(Box::new((
                self,
                OrdinaryProjectionError::SequenceRelocation,
            )));
        };
        let Some(sequence_delta) = marker_count.checked_add(1) else {
            return Err(Box::new((
                self,
                OrdinaryProjectionError::SequenceRelocation,
            )));
        };
        if let Err(error) = preflight_ordinary_sequence_owners(&self.sequence, sequence_delta) {
            return Err(Box::new((self, error)));
        }
        if let Err(error) = preflight_ordinary_order_owners(&self.order) {
            return Err(Box::new((self, error)));
        }
        let (
            floor,
            retained_charge,
            baseline,
            accounting,
            required_capacity,
            order,
            sequence,
            caller_record,
            caller_charge,
            retained_records,
            retained_charges,
            new_marker_candidates,
        ) = projected.into_parts();

        let prior_sequence_ledger = self.sequence.ledger;
        let prior_order_ledger = self.order.ledger;
        relay_ordinary_sequence_owners(&mut self.sequence, sequence_delta);
        relay_ordinary_order_owners(&mut self.order);

        self.sequence.ledger = sequence.resulting();
        self.sequence.immutable_candidates.extend(
            new_marker_candidates
                .iter()
                .copied()
                .map(ImmutableSequenceCandidate::Marker),
        );
        self.order.ledger = order.resulting();
        if !new_marker_candidates.is_empty() {
            self.order
                .immutable_candidates
                .push(ImmutableOrderCandidateMajor {
                    transaction_order: order.major(),
                    candidate_keys: new_marker_candidates
                        .iter()
                        .map(|candidate| candidate.admission_order)
                        .collect(),
                });
        }
        if validate_cross_counter(&self.sequence, &self.order).is_err() {
            self.sequence.ledger = prior_sequence_ledger;
            self.sequence.immutable_candidates.clear();
            self.order.ledger = prior_order_ledger;
            self.order.immutable_candidates.clear();
            rollback_ordinary_sequence_owners(&mut self.sequence, sequence_delta);
            rollback_ordinary_order_owners(&mut self.order);
            return Err(Box::new((
                self,
                OrdinaryProjectionError::SequenceRelocation,
            )));
        }
        self.retained_floor = floor.resulting_floor;
        self.retained_records = retained_records;
        self.marker_records
            .retain(|record| u128::from(record.delivery_seq) >= floor.resulting_floor);

        Ok(ProjectedOrdinaryRecord {
            frontiers: self,
            floor,
            retained_charge,
            baseline,
            accounting,
            required_capacity,
            order,
            sequence,
            observer_floor,
            closure,
            caller_record,
            caller_charge,
            retained_charges,
            new_marker_candidates,
        })
    }

    /// Returns the exact causal key a settled bound/detached Leave would
    /// consume without relinquishing frontier authority.
    ///
    /// This planning view exists so a durable binding can compute the
    /// canonical keyed `Left` row charge before calling the consuming commit.
    /// The consuming preparation reruns the same validation.
    ///
    /// # Errors
    ///
    /// Returns [`PrepareLeaveAuthorityError`] under the same preconditions as
    /// [`Self::prepare_settled_leave_authority`].
    pub fn planned_settled_leave_admission_order<F>(
        &self,
        member: &LiveMember<F>,
        binding_state: BindingState,
    ) -> Result<super::AdmissionOrder, PrepareLeaveAuthorityError> {
        let (participant_id, ended_binding_epoch) =
            validate_settled_leave_prestate(self, member, binding_state)?;
        let selection = select_leave_order(&self.order, participant_id, ended_binding_epoch, None)?;
        Ok(super::AdmissionOrder::new(
            selection.selected_major,
            CandidatePhase::MembershipExit,
            participant_id,
        ))
    }

    /// Consumes the exact settled bound/detached `X` authority and relays the
    /// surviving order lane behind the selected `Left` major.
    ///
    /// Bound Leave also invalidates the same participant's exact `A` handle.
    /// Every immutable candidate must already have drained. The returned
    /// authority owns this frontier snapshot and is intentionally non-cloneable;
    /// only [`super::commit_leave`] can consume it.
    ///
    /// # Errors
    ///
    /// Returns [`PrepareLeaveAuthorityError`] when identity/binding authority,
    /// candidate precedence, a logical handle, or checked relay capacity fails.
    pub fn prepare_settled_leave_authority<F>(
        mut self,
        member: &LiveMember<F>,
        binding_state: BindingState,
    ) -> Result<PreparedLeaveAuthority, PrepareLeaveAuthorityError> {
        let (participant_id, ended_binding_epoch) =
            validate_settled_leave_prestate(&self, member, binding_state)?;
        let left_transaction_order =
            consume_leave_order_lane(&mut self.order, participant_id, ended_binding_epoch, None)?;
        Ok(PreparedLeaveAuthority::settled(
            self,
            member.conversation_id(),
            participant_id,
            ended_binding_epoch,
            left_transaction_order,
        ))
    }

    /// Returns the exact causal key a pending-terminal Leave would consume
    /// after its immutable terminal, without relinquishing frontier authority.
    ///
    /// # Errors
    ///
    /// Returns [`PrepareLeaveAuthorityError`] under the same preconditions as
    /// [`Self::prepare_pending_leave_authority`].
    pub fn planned_pending_leave_admission_order<F>(
        &self,
        member: &LiveMember<F>,
        pending: PendingFinalization,
    ) -> Result<super::AdmissionOrder, PrepareLeaveAuthorityError> {
        let (participant_id, expected_order) =
            validate_pending_leave_prestate(self, member, pending)?;
        let selection =
            select_leave_order(&self.order, participant_id, None, Some(expected_order))?;
        Ok(super::AdmissionOrder::new(
            selection.selected_major,
            CandidatePhase::MembershipExit,
            participant_id,
        ))
    }

    /// Consumes the exact pending-terminal plus `X` positional order authority.
    ///
    /// The pending terminal must be the sole immutable candidate, must match the
    /// detached identity's exact prior epoch, and must lie strictly before the
    /// participant's `X` handle. The returned non-cloneable authority owns the
    /// relayed frontier snapshot and can be consumed only by
    /// [`super::commit_pending_leave`].
    ///
    /// # Errors
    ///
    /// Returns [`PrepareLeaveAuthorityError`] for mismatched identity/binding,
    /// any unrelated candidate, absent logical ownership, or insufficient
    /// checked suffix for the later-handle relocation.
    pub fn prepare_pending_leave_authority<F>(
        mut self,
        member: &LiveMember<F>,
        pending: PendingFinalization,
    ) -> Result<PreparedLeaveAuthority, PrepareLeaveAuthorityError> {
        let (participant_id, expected_order) =
            validate_pending_leave_prestate(&self, member, pending)?;
        let left_transaction_order =
            consume_leave_order_lane(&mut self.order, participant_id, None, Some(expected_order))?;
        Ok(PreparedLeaveAuthority::pending(
            self,
            member.conversation_id(),
            participant_id,
            pending.binding_epoch(),
            expected_order,
            left_transaction_order,
        ))
    }

    /// Completes the claim-frontier portion of one already-authorized Leave.
    ///
    /// The transition consumes the retiring identity's `E`, its still-live `T`
    /// when applicable, and every product dimension removed with membership.
    /// Surviving direct, product, and recovery claims are relayed gap-free after
    /// the appended `Left` (or pending terminal plus `Left`) records. The
    /// retained suffix is extended at the unchanged floor so no caller-authored
    /// floor or snapshot can be substituted for the protocol result.
    #[allow(
        clippy::too_many_lines,
        reason = "the atomic Leave relay keeps membership, both ledgers, products, recovery, and retained rows visibly in one checked transition"
    )]
    pub(super) fn finish_leave_claims(
        mut self,
        participant_id: ParticipantId,
        ended_binding_epoch: Option<BindingEpoch>,
        committed_terminal: Option<CommittedBindingTerminal>,
        left_delivery_seq: DeliverySeq,
        left_transaction_order: TransactionOrder,
    ) -> Result<Self, LeaveCommitError> {
        let prior_high = self.sequence.ledger.high_watermark();
        let first_appended = prior_high
            .checked_add(1)
            .ok_or(LeaveCommitError::SequenceAuthority)?;
        let expected_left = if committed_terminal.is_some() {
            first_appended
                .checked_add(1)
                .ok_or(LeaveCommitError::SequenceAuthority)?
        } else {
            first_appended
        };
        if left_delivery_seq != expected_left {
            return Err(LeaveCommitError::SequenceAuthority);
        }

        match (
            committed_terminal,
            self.sequence.immutable_candidates.as_slice(),
        ) {
            (
                Some(terminal),
                [
                    ImmutableSequenceCandidate::BindingTerminal {
                        delivery_seq,
                        admission_order,
                        owner,
                    },
                ],
            ) if *delivery_seq == first_appended
                && terminal.delivery_seq() == first_appended
                && *admission_order == terminal.admission_order()
                && owner.participant_index == participant_id
                && owner.binding_epoch == terminal.binding_epoch() => {}
            (None, []) => {}
            (Some(_) | None, _) => return Err(LeaveCommitError::SequenceAuthority),
        }

        let Some(active_index) = self
            .active_identities
            .participants
            .iter()
            .position(|participant| participant.participant_index == participant_id)
        else {
            return Err(LeaveCommitError::ResultingFrontier);
        };
        self.active_identities.participants.remove(active_index);
        let resulting_live = usize_to_u64(self.active_identities.participants.len());

        let mut exit_consumed = false;
        let mut terminal_consumed = ended_binding_epoch.is_none();
        let mut units = Vec::new();
        for claim in self.sequence.movable_claims.iter().copied() {
            match claim.owner {
                SequenceDirectOwner::MembershipExit {
                    participant_index: owner,
                } if owner == participant_id => {
                    if exit_consumed {
                        return Err(LeaveCommitError::ResultingFrontier);
                    }
                    exit_consumed = true;
                }
                SequenceDirectOwner::BindingTerminal(owner)
                    if owner.participant_index == participant_id
                        && Some(owner.binding_epoch) == ended_binding_epoch =>
                {
                    if terminal_consumed {
                        return Err(LeaveCommitError::ResultingFrontier);
                    }
                    terminal_consumed = true;
                }
                SequenceDirectOwner::MembershipExit { .. }
                | SequenceDirectOwner::BindingTerminal(_) => {
                    units.push(LeaveSequenceUnit::Direct(claim));
                }
            }
        }
        if !exit_consumed || !terminal_consumed {
            return Err(LeaveCommitError::ResultingFrontier);
        }

        let recovery_owned = self
            .sequence
            .recovery
            .is_some_and(|block| block.participant_index == participant_id);
        for range in self.sequence.products.live_times_terminal.iter().copied() {
            if range.terminal.participant_index != participant_id && resulting_live != 0 {
                units.push(LeaveSequenceUnit::TerminalProduct(TerminalProductRange {
                    start: range.start,
                    length: resulting_live,
                    terminal: range.terminal,
                }));
            }
        }
        if let Some(range) = self.sequence.products.live_times_replacement_terminal
            && !recovery_owned
            && resulting_live != 0
        {
            units.push(LeaveSequenceUnit::ReplacementProduct(
                ReplacementTerminalProductRange {
                    start: range.start,
                    length: resulting_live,
                    participant_index: range.participant_index,
                    marker_delivery_seq: range.marker_delivery_seq,
                    prior_binding_epoch: range.prior_binding_epoch,
                },
            ));
        }
        let resulting_other = resulting_live.saturating_sub(1);
        if resulting_other != 0 {
            for range in self.sequence.products.other_live_times_exit.iter().copied() {
                if range.exit_participant != participant_id {
                    units.push(LeaveSequenceUnit::ExitProduct(ExitProductRange {
                        start: range.start,
                        length: resulting_other,
                        exit_participant: range.exit_participant,
                    }));
                }
            }
        }
        if let Some(recovery) = self.sequence.recovery
            && !recovery_owned
        {
            units.push(LeaveSequenceUnit::Recovery(recovery));
        }
        units.sort_by_key(|unit| unit.original_start());

        let mut cursor = left_delivery_seq.checked_add(1);
        let mut movable_claims = Vec::new();
        let mut terminal_products = Vec::new();
        let mut replacement_product = None;
        let mut exit_products = Vec::new();
        let mut recovery = None;
        for unit in units {
            match unit {
                LeaveSequenceUnit::Direct(mut claim) => {
                    claim.delivery_seq = allocate_leave_sequence_range(&mut cursor, 1)?;
                    movable_claims.push(claim);
                }
                LeaveSequenceUnit::TerminalProduct(mut range) => {
                    range.start = allocate_leave_sequence_range(&mut cursor, range.length)?;
                    terminal_products.push(range);
                }
                LeaveSequenceUnit::ReplacementProduct(mut range) => {
                    range.start = allocate_leave_sequence_range(&mut cursor, range.length)?;
                    replacement_product = Some(range);
                }
                LeaveSequenceUnit::ExitProduct(mut range) => {
                    range.start = allocate_leave_sequence_range(&mut cursor, range.length)?;
                    exit_products.push(range);
                }
                LeaveSequenceUnit::Recovery(mut block) => {
                    let length = 2 + u64::from(block.terminal.is_some());
                    let start = allocate_leave_sequence_range(&mut cursor, length)?;
                    if let Some(mut terminal) = block.terminal {
                        terminal.delivery_seq = start;
                        block.terminal = Some(terminal);
                        block.recovery_attach_seq = start
                            .checked_add(1)
                            .ok_or(LeaveCommitError::ResultingFrontier)?;
                    } else {
                        block.recovery_attach_seq = start;
                    }
                    block.replacement_terminal_seq = block
                        .recovery_attach_seq
                        .checked_add(1)
                        .ok_or(LeaveCommitError::ResultingFrontier)?;
                    recovery = Some(block);
                }
            }
        }
        movable_claims.sort_by_key(|claim| claim.delivery_seq);
        terminal_products.sort_by_key(|range| range.start);
        exit_products.sort_by_key(|range| range.start);
        let terminal_count = usize_to_u64(
            movable_claims
                .iter()
                .filter(|claim| matches!(claim.owner, SequenceDirectOwner::BindingTerminal(_)))
                .count(),
        ) + u64::from(recovery.is_some_and(|block| block.terminal.is_some()));
        let recovery_reserve = if recovery.is_some() {
            RecoverySequenceReserve::DetachedCredentialRecovery
        } else {
            RecoverySequenceReserve::None
        };
        let ledger = SequenceLedger::try_new(
            left_delivery_seq,
            SequenceClaims::new(resulting_live, terminal_count, 0, recovery_reserve),
        )
        .map_err(|_| LeaveCommitError::ResultingFrontier)?;
        self.sequence = SequenceClaimFrontier {
            ledger,
            movable_claims,
            immutable_candidates: Vec::new(),
            products: SequenceProductRanges {
                live_times_terminal: terminal_products,
                live_times_replacement_terminal: replacement_product,
                other_live_times_exit: exit_products,
            },
            recovery,
        };

        if let Some(terminal) = committed_terminal {
            self.retained_records.push(RetainedCausalRecord {
                delivery_seq: terminal.delivery_seq(),
                admission_order: terminal.admission_order(),
                kind: RetainedCausalRecordKind::BindingTerminal(BindingTerminalOwner {
                    participant_index: participant_id,
                    binding_epoch: terminal.binding_epoch(),
                }),
            });
        }
        self.retained_records.push(RetainedCausalRecord {
            delivery_seq: left_delivery_seq,
            admission_order: super::AdmissionOrder::new(
                left_transaction_order,
                CandidatePhase::MembershipExit,
                participant_id,
            ),
            kind: RetainedCausalRecordKind::MembershipExit {
                participant_index: participant_id,
            },
        });
        self.retained_records
            .sort_by_key(|record| record.delivery_seq);

        let order_claims = self.order.ledger.claims();
        if order_claims.membership_exits() != resulting_live
            || self.order.recovery.is_some() != self.sequence.recovery.is_some()
            || validate_cross_counter(&self.sequence, &self.order).is_err()
        {
            return Err(LeaveCommitError::ResultingFrontier);
        }
        Ok(self)
    }

    fn marker_candidate(&self, delivery_seq: DeliverySeq) -> Option<ValidatedMarkerCandidate> {
        self.sequence
            .immutable_candidates
            .iter()
            .find_map(|candidate| match candidate {
                ImmutableSequenceCandidate::Marker(candidate)
                    if candidate.delivery_seq == delivery_seq =>
                {
                    Some(ValidatedMarkerCandidate {
                        conversation_id: self.conversation_id,
                        candidate: *candidate,
                        seal: MarkerAuthoritySeal::Validated,
                    })
                }
                _ => None,
            })
    }

    /// Consumes only the exact next bound marker candidate and atomically
    /// materializes its retained marker fact.
    ///
    /// The delivery high watermark advances once, `M` decreases once, and all
    /// surviving numeric owners remain in place because they already begin at
    /// the new `H+1`. The shared causal order major is already allocated and is
    /// therefore removed only from the immutable tuple lane; marker drain never
    /// allocates or advances [`OrderHigh`].
    pub(super) fn drain_next_marker_core(
        mut self,
    ) -> Result<MarkerDrainCore, MarkerDrainCoreError> {
        let Some(first) = self.sequence.immutable_candidates.first().copied() else {
            return Err(MarkerDrainCoreError::NoCandidate);
        };
        let ImmutableSequenceCandidate::Marker(marker) = first else {
            return Err(MarkerDrainCoreError::BindingTerminalFirst);
        };
        let expected_sequence = self
            .sequence
            .ledger
            .high_watermark()
            .checked_add(1)
            .ok_or(MarkerDrainCoreError::SequenceNotNext)?;
        if marker.delivery_seq != expected_sequence {
            return Err(MarkerDrainCoreError::SequenceNotNext);
        }
        if order_is_above_high(
            marker.admission_order.transaction_order(),
            self.order.ledger.high(),
        ) {
            return Err(MarkerDrainCoreError::CausalMajorNotAllocated);
        }
        let candidate = self
            .marker_candidate(marker.delivery_seq)
            .ok_or(MarkerDrainCoreError::NoCandidate)?;

        let key = marker.admission_order;
        let Some(group_index) = self
            .order
            .immutable_candidates
            .iter()
            .position(|group| group.candidate_keys.contains(&key))
        else {
            return Err(MarkerDrainCoreError::MissingOrderCandidate);
        };
        let group = &mut self.order.immutable_candidates[group_index];
        let Ok(key_index) = group.candidate_keys.binary_search(&key) else {
            return Err(MarkerDrainCoreError::MissingOrderCandidate);
        };
        group.candidate_keys.remove(key_index);
        if group.candidate_keys.is_empty() {
            self.order.immutable_candidates.remove(group_index);
        }
        self.sequence.immutable_candidates.remove(0);

        let claims = self.sequence.ledger.claims();
        let markers = claims
            .markers()
            .checked_sub(1)
            .ok_or(MarkerDrainCoreError::ResultingLedger)?;
        self.sequence.ledger = SequenceLedger::try_new(
            expected_sequence,
            super::SequenceClaims::new(
                claims.live_members(),
                claims.binding_terminals(),
                markers,
                claims.recovery(),
            ),
        )
        .map_err(|_| MarkerDrainCoreError::ResultingLedger)?;

        let record = RetainedCausalRecord {
            delivery_seq: marker.delivery_seq,
            admission_order: marker.admission_order,
            kind: RetainedCausalRecordKind::CompactionMarker {
                participant_index: marker.admission_order.participant_index(),
                provenance: marker.provenance,
            },
        };
        self.marker_records.push(record);
        self.retained_records.push(record);
        Ok(MarkerDrainCore {
            candidate,
            record: ValidatedMarkerRecord {
                conversation_id: self.conversation_id,
                record,
                provenance: marker.provenance,
                target_binding: marker.target_binding,
                occurrence: MarkerRecordOccurrence::Undelivered,
                seal: MarkerAuthoritySeal::Validated,
            },
            frontiers: self,
        })
    }
}

fn rebuild_unreserved_frontiers(
    active: &ActiveIdentityRanks,
    sequence_ledger: SequenceLedger,
    order_ledger: OrderLedger,
) -> Result<(SequenceClaimFrontier, OrderClaimFrontier), LiveFrontierTransitionError> {
    let terminal_owners: Vec<_> = active
        .participants()
        .iter()
        .filter_map(|participant| match participant.binding() {
            FrontierBinding::Bound(binding_epoch) => Some(BindingTerminalOwner {
                participant_index: participant.participant_index(),
                binding_epoch,
            }),
            FrontierBinding::Detached(_) => None,
        })
        .collect();
    let live_count = active.len();
    let terminal_count =
        u64::try_from(terminal_owners.len()).map_err(|_| LiveFrontierTransitionError::Exhausted)?;
    let sequence_claims = sequence_ledger.claims();
    let order_claims = order_ledger.claims();
    if sequence_claims.live_members() != live_count
        || sequence_claims.binding_terminals() != terminal_count
        || sequence_claims.markers() != 0
        || sequence_claims.recovery() != RecoverySequenceReserve::None
        || order_claims.active_binding_terminals() != terminal_count
        || order_claims.membership_exits() != live_count
        || order_claims.recovery_operation()
        || order_claims.recovery_replacement_terminal()
    {
        return Err(LiveFrontierTransitionError::ResultingFrontier);
    }

    let sequence = rebuild_unreserved_sequence(active, &terminal_owners, sequence_ledger)?;
    let order = rebuild_unreserved_order(active, &terminal_owners, order_ledger)?;
    validate_cross_counter(&sequence, &order)
        .map_err(|_| LiveFrontierTransitionError::ResultingFrontier)?;
    Ok((sequence, order))
}

fn rebuild_unreserved_sequence(
    active: &ActiveIdentityRanks,
    terminal_owners: &[BindingTerminalOwner],
    sequence_ledger: SequenceLedger,
) -> Result<SequenceClaimFrontier, LiveFrontierTransitionError> {
    let live_count = active.len();
    let mut sequence_cursor = sequence_ledger
        .high_watermark()
        .checked_add(1)
        .ok_or(LiveFrontierTransitionError::Exhausted)?;
    let mut movable_sequence = Vec::new();
    for terminal in terminal_owners {
        movable_sequence.push(MovableSequenceClaim {
            delivery_seq: take_live_sequence(&mut sequence_cursor, 1)?,
            owner: SequenceDirectOwner::BindingTerminal(*terminal),
        });
    }
    for participant in active.participants() {
        movable_sequence.push(MovableSequenceClaim {
            delivery_seq: take_live_sequence(&mut sequence_cursor, 1)?,
            owner: SequenceDirectOwner::MembershipExit {
                participant_index: participant.participant_index(),
            },
        });
    }
    let mut terminal_products = Vec::new();
    for terminal in terminal_owners {
        terminal_products.push(TerminalProductRange {
            start: take_live_sequence(&mut sequence_cursor, live_count)?,
            length: live_count,
            terminal: *terminal,
        });
    }
    let exit_product_length = live_count.saturating_sub(1);
    let mut exit_products = Vec::new();
    for participant in active.participants() {
        exit_products.push(ExitProductRange {
            start: take_live_sequence(&mut sequence_cursor, exit_product_length)?,
            length: exit_product_length,
            exit_participant: participant.participant_index(),
        });
    }
    let sequence_end = u128::from(sequence_ledger.high_watermark())
        .checked_add(sequence_ledger.required_reserve())
        .and_then(|value| value.checked_add(1))
        .ok_or(LiveFrontierTransitionError::Exhausted)?;
    if u128::from(sequence_cursor) != sequence_end {
        return Err(LiveFrontierTransitionError::ResultingFrontier);
    }
    Ok(SequenceClaimFrontier {
        ledger: sequence_ledger,
        movable_claims: movable_sequence,
        immutable_candidates: Vec::new(),
        products: SequenceProductRanges {
            live_times_terminal: terminal_products,
            live_times_replacement_terminal: None,
            other_live_times_exit: exit_products,
        },
        recovery: None,
    })
}

fn rebuild_unreserved_order(
    active: &ActiveIdentityRanks,
    terminal_owners: &[BindingTerminalOwner],
    order_ledger: OrderLedger,
) -> Result<OrderClaimFrontier, LiveFrontierTransitionError> {
    let order_claims = order_ledger.claims();
    let order_start = order_frontier_start(order_ledger.high());
    let mut order_cursor =
        u64::try_from(order_start).map_err(|_| LiveFrontierTransitionError::Exhausted)?;
    let mut movable_order = Vec::new();
    for terminal in terminal_owners.iter().copied() {
        movable_order.push(MovableOrderClaim {
            transaction_order: take_live_order(&mut order_cursor)?,
            owner: OrderDirectOwner::ActiveBindingTerminal(terminal),
        });
    }
    for participant in active.participants() {
        movable_order.push(MovableOrderClaim {
            transaction_order: take_live_order(&mut order_cursor)?,
            owner: OrderDirectOwner::MembershipExit {
                participant_index: participant.participant_index(),
            },
        });
    }
    if u128::from(order_cursor) != order_start + order_claims.total() {
        return Err(LiveFrontierTransitionError::ResultingFrontier);
    }
    Ok(OrderClaimFrontier {
        ledger: order_ledger,
        movable_claims: movable_order,
        immutable_candidates: Vec::new(),
        recovery: None,
    })
}

fn take_live_sequence(
    cursor: &mut DeliverySeq,
    length: u64,
) -> Result<DeliverySeq, LiveFrontierTransitionError> {
    let start = *cursor;
    *cursor = cursor
        .checked_add(length)
        .ok_or(LiveFrontierTransitionError::Exhausted)?;
    Ok(start)
}

fn take_live_order(
    cursor: &mut TransactionOrder,
) -> Result<TransactionOrder, LiveFrontierTransitionError> {
    let value = *cursor;
    *cursor = cursor
        .checked_add(1)
        .ok_or(LiveFrontierTransitionError::Exhausted)?;
    Ok(value)
}

fn ordinary_unaccepted_marker_anchors(frontiers: &ClaimFrontiers) -> Vec<DeliverySeq> {
    frontiers
        .marker_records
        .iter()
        .filter_map(|record| {
            let RetainedCausalRecordKind::CompactionMarker {
                participant_index, ..
            } = record.kind
            else {
                return None;
            };
            active_participant(&frontiers.active_identities, participant_index)
                .is_some_and(|participant| participant.cursor < record.delivery_seq)
                .then_some(record.delivery_seq)
        })
        .collect()
}

fn projection_failure(
    frontiers: ClaimFrontiers,
    input: OrdinaryRecordProjectionInput,
    error: OrdinaryProjectionError,
) -> Box<OrdinaryRecordProjectionFailure> {
    Box::new(OrdinaryRecordProjectionFailure {
        frontiers,
        input,
        error,
    })
}

fn preflight_ordinary_sequence_owners(
    sequence: &SequenceClaimFrontier,
    delta: u64,
) -> Result<(), OrdinaryProjectionError> {
    if !sequence.immutable_candidates.is_empty() {
        return Err(OrdinaryProjectionError::SequenceRelocation);
    }
    for claim in &sequence.movable_claims {
        claim
            .delivery_seq
            .checked_add(delta)
            .ok_or(OrdinaryProjectionError::SequenceRelocation)?;
    }
    for range in &sequence.products.live_times_terminal {
        range
            .start
            .checked_add(delta)
            .ok_or(OrdinaryProjectionError::SequenceRelocation)?;
    }
    if let Some(range) = &sequence.products.live_times_replacement_terminal {
        range
            .start
            .checked_add(delta)
            .ok_or(OrdinaryProjectionError::SequenceRelocation)?;
    }
    for range in &sequence.products.other_live_times_exit {
        range
            .start
            .checked_add(delta)
            .ok_or(OrdinaryProjectionError::SequenceRelocation)?;
    }
    if let Some(recovery) = &sequence.recovery {
        if let Some(terminal) = &recovery.terminal {
            terminal
                .delivery_seq
                .checked_add(delta)
                .ok_or(OrdinaryProjectionError::SequenceRelocation)?;
        }
        recovery
            .recovery_attach_seq
            .checked_add(delta)
            .ok_or(OrdinaryProjectionError::SequenceRelocation)?;
        recovery
            .replacement_terminal_seq
            .checked_add(delta)
            .ok_or(OrdinaryProjectionError::SequenceRelocation)?;
    }
    Ok(())
}

fn relay_ordinary_sequence_owners(sequence: &mut SequenceClaimFrontier, delta: u64) {
    for claim in &mut sequence.movable_claims {
        claim.delivery_seq = claim.delivery_seq.wrapping_add(delta);
    }
    for range in &mut sequence.products.live_times_terminal {
        range.start = range.start.wrapping_add(delta);
    }
    if let Some(range) = &mut sequence.products.live_times_replacement_terminal {
        range.start = range.start.wrapping_add(delta);
    }
    for range in &mut sequence.products.other_live_times_exit {
        range.start = range.start.wrapping_add(delta);
    }
    if let Some(recovery) = &mut sequence.recovery {
        if let Some(terminal) = &mut recovery.terminal {
            terminal.delivery_seq = terminal.delivery_seq.wrapping_add(delta);
        }
        recovery.recovery_attach_seq = recovery.recovery_attach_seq.wrapping_add(delta);
        recovery.replacement_terminal_seq = recovery.replacement_terminal_seq.wrapping_add(delta);
    }
}

fn rollback_ordinary_sequence_owners(sequence: &mut SequenceClaimFrontier, delta: u64) {
    for claim in &mut sequence.movable_claims {
        claim.delivery_seq = claim.delivery_seq.wrapping_sub(delta);
    }
    for range in &mut sequence.products.live_times_terminal {
        range.start = range.start.wrapping_sub(delta);
    }
    if let Some(range) = &mut sequence.products.live_times_replacement_terminal {
        range.start = range.start.wrapping_sub(delta);
    }
    for range in &mut sequence.products.other_live_times_exit {
        range.start = range.start.wrapping_sub(delta);
    }
    if let Some(recovery) = &mut sequence.recovery {
        if let Some(terminal) = &mut recovery.terminal {
            terminal.delivery_seq = terminal.delivery_seq.wrapping_sub(delta);
        }
        recovery.recovery_attach_seq = recovery.recovery_attach_seq.wrapping_sub(delta);
        recovery.replacement_terminal_seq = recovery.replacement_terminal_seq.wrapping_sub(delta);
    }
}

fn preflight_ordinary_order_owners(
    order: &OrderClaimFrontier,
) -> Result<(), OrdinaryProjectionError> {
    if !order.immutable_candidates.is_empty() {
        return Err(OrdinaryProjectionError::OrderRelocation);
    }
    for claim in &order.movable_claims {
        claim
            .transaction_order
            .checked_add(1)
            .ok_or(OrdinaryProjectionError::OrderRelocation)?;
    }
    if let Some(recovery) = &order.recovery {
        if let Some(active_binding) = &recovery.active_binding {
            active_binding
                .transaction_order
                .checked_add(1)
                .ok_or(OrdinaryProjectionError::OrderRelocation)?;
        }
        recovery
            .recovery_operation_order
            .checked_add(1)
            .ok_or(OrdinaryProjectionError::OrderRelocation)?;
        recovery
            .replacement_terminal_order
            .checked_add(1)
            .ok_or(OrdinaryProjectionError::OrderRelocation)?;
    }
    Ok(())
}

fn relay_ordinary_order_owners(order: &mut OrderClaimFrontier) {
    for claim in &mut order.movable_claims {
        claim.transaction_order = claim.transaction_order.wrapping_add(1);
    }
    if let Some(recovery) = &mut order.recovery {
        if let Some(active_binding) = &mut recovery.active_binding {
            active_binding.transaction_order = active_binding.transaction_order.wrapping_add(1);
        }
        recovery.recovery_operation_order = recovery.recovery_operation_order.wrapping_add(1);
        recovery.replacement_terminal_order = recovery.replacement_terminal_order.wrapping_add(1);
    }
}

fn rollback_ordinary_order_owners(order: &mut OrderClaimFrontier) {
    for claim in &mut order.movable_claims {
        claim.transaction_order = claim.transaction_order.wrapping_sub(1);
    }
    if let Some(recovery) = &mut order.recovery {
        if let Some(active_binding) = &mut recovery.active_binding {
            active_binding.transaction_order = active_binding.transaction_order.wrapping_sub(1);
        }
        recovery.recovery_operation_order = recovery.recovery_operation_order.wrapping_sub(1);
        recovery.replacement_terminal_order = recovery.replacement_terminal_order.wrapping_sub(1);
    }
}

fn validate_leave_identity<F>(
    frontiers: &ClaimFrontiers,
    member: &LiveMember<F>,
) -> Result<(), PrepareLeaveAuthorityError> {
    if member.conversation_id() != frontiers.conversation_id {
        return Err(PrepareLeaveAuthorityError::Conversation);
    }
    let Some(participant) =
        active_participant(&frontiers.active_identities, member.participant_id())
    else {
        return Err(PrepareLeaveAuthorityError::Identity);
    };
    if participant.cursor != member.cursor() {
        return Err(PrepareLeaveAuthorityError::Identity);
    }
    Ok(())
}

fn validate_settled_leave_prestate<F>(
    frontiers: &ClaimFrontiers,
    member: &LiveMember<F>,
    binding_state: BindingState,
) -> Result<(ParticipantId, Option<BindingEpoch>), PrepareLeaveAuthorityError> {
    let participant_id = member.participant_id();
    validate_leave_identity(frontiers, member)?;
    if !frontiers.order.immutable_candidates.is_empty() {
        return Err(PrepareLeaveAuthorityError::ImmutablePrefix);
    }
    let ended_binding_epoch = match binding_state {
        BindingState::Detached => {
            let Some(participant) =
                active_participant(&frontiers.active_identities, participant_id)
            else {
                return Err(PrepareLeaveAuthorityError::Identity);
            };
            if !matches!(participant.binding, FrontierBinding::Detached(_)) {
                return Err(PrepareLeaveAuthorityError::Binding);
            }
            None
        }
        BindingState::Bound(binding)
            if binding.conversation_id == frontiers.conversation_id
                && binding.participant_id == participant_id =>
        {
            let Some(participant) =
                active_participant(&frontiers.active_identities, participant_id)
            else {
                return Err(PrepareLeaveAuthorityError::Identity);
            };
            if participant.binding != FrontierBinding::Bound(binding.binding_epoch) {
                return Err(PrepareLeaveAuthorityError::Binding);
            }
            Some(binding.binding_epoch)
        }
        BindingState::Bound(_) | BindingState::PendingFinalization(_) => {
            return Err(PrepareLeaveAuthorityError::Binding);
        }
    };
    Ok((participant_id, ended_binding_epoch))
}

fn validate_pending_leave_prestate<F>(
    frontiers: &ClaimFrontiers,
    member: &LiveMember<F>,
    pending: PendingFinalization,
) -> Result<(ParticipantId, super::AdmissionOrder), PrepareLeaveAuthorityError> {
    let participant_id = member.participant_id();
    validate_leave_identity(frontiers, member)?;
    if pending.conversation_id() != frontiers.conversation_id
        || pending.participant_id() != participant_id
    {
        return Err(PrepareLeaveAuthorityError::Binding);
    }
    let Some(participant) = active_participant(&frontiers.active_identities, participant_id) else {
        return Err(PrepareLeaveAuthorityError::Identity);
    };
    if participant.binding != FrontierBinding::Detached(pending.binding_epoch()) {
        return Err(PrepareLeaveAuthorityError::Binding);
    }
    let expected_order = pending.admission_order();
    let exact_sequence_candidate = matches!(
        frontiers.sequence.immutable_candidates.as_slice(),
        [ImmutableSequenceCandidate::BindingTerminal {
            admission_order,
            owner,
            ..
        }] if *admission_order == expected_order
            && owner.participant_index == participant_id
            && owner.binding_epoch == pending.binding_epoch()
    );
    let exact_order_candidate = matches!(
        frontiers.order.immutable_candidates.as_slice(),
        [ImmutableOrderCandidateMajor {
            transaction_order,
            candidate_keys,
        }] if *transaction_order == expected_order.transaction_order()
            && candidate_keys.as_slice() == [expected_order]
    );
    if !exact_sequence_candidate || !exact_order_candidate {
        return Err(PrepareLeaveAuthorityError::PendingCandidate);
    }
    Ok((participant_id, expected_order))
}

#[derive(Clone, Copy)]
enum LeaveRelayUnit {
    Direct(MovableOrderClaim),
    Recovery(RecoveryOrderBlock),
}

impl LeaveRelayUnit {
    fn start(self) -> TransactionOrder {
        match self {
            Self::Direct(claim) => claim.transaction_order,
            Self::Recovery(block) => block_start_validated_order(block),
        }
    }

    const fn len(self) -> u64 {
        match self {
            Self::Direct(_) => 1,
            Self::Recovery(block) => match block.active_binding {
                Some(_) => 3,
                None => 2,
            },
        }
    }
}

struct LeaveOrderSelection {
    units: Vec<LeaveRelayUnit>,
    selected_major: TransactionOrder,
}

fn select_leave_order(
    order: &OrderClaimFrontier,
    participant_id: ParticipantId,
    ended_binding_epoch: Option<BindingEpoch>,
    pending_order: Option<super::AdmissionOrder>,
) -> Result<LeaveOrderSelection, PrepareLeaveAuthorityError> {
    let Some(exit_index) = order.movable_claims.iter().position(|claim| {
        claim.owner
            == OrderDirectOwner::MembershipExit {
                participant_index: participant_id,
            }
    }) else {
        return Err(PrepareLeaveAuthorityError::MembershipExitClaim);
    };
    let exit_claim = order.movable_claims[exit_index];
    if pending_order
        .is_some_and(|pending| pending.transaction_order() >= exit_claim.transaction_order)
    {
        return Err(PrepareLeaveAuthorityError::PendingCandidate);
    }
    let active_index = matching_active_claim(order, participant_id, ended_binding_epoch)?;
    let mut units = Vec::new();
    for (index, claim) in order.movable_claims.iter().copied().enumerate() {
        if index != exit_index && Some(index) != active_index {
            units.push(LeaveRelayUnit::Direct(claim));
        }
    }
    if let Some(recovery) = order
        .recovery
        .filter(|recovery| recovery.participant_index != participant_id)
    {
        units.push(LeaveRelayUnit::Recovery(recovery));
    }
    units.sort_by_key(|unit| unit.start());
    let surviving_handles: u128 = units.iter().map(|unit| u128::from(unit.len())).sum();
    let exit_major = exit_claim.transaction_order;
    let later_handle_fits = u128::from(u64::MAX - exit_major) >= surviving_handles;
    let selected_major = if later_handle_fits {
        exit_major
    } else if pending_order.is_some() {
        return Err(PrepareLeaveAuthorityError::OrderCapacity);
    } else {
        u64::try_from(order_frontier_start(order.ledger.high()))
            .map_err(|_| PrepareLeaveAuthorityError::OrderCapacity)?
    };
    Ok(LeaveOrderSelection {
        units,
        selected_major,
    })
}

fn matching_active_claim(
    order: &OrderClaimFrontier,
    participant_id: ParticipantId,
    ended_binding_epoch: Option<BindingEpoch>,
) -> Result<Option<usize>, PrepareLeaveAuthorityError> {
    let Some(binding_epoch) = ended_binding_epoch else {
        return Ok(None);
    };
    let expected = OrderDirectOwner::ActiveBindingTerminal(BindingTerminalOwner {
        participant_index: participant_id,
        binding_epoch,
    });
    order
        .movable_claims
        .iter()
        .position(|claim| claim.owner == expected)
        .map(Some)
        .ok_or(PrepareLeaveAuthorityError::ActiveBindingClaim)
}

fn relay_leave_order_units(
    units: Vec<LeaveRelayUnit>,
    selected_major: TransactionOrder,
) -> Result<(Vec<MovableOrderClaim>, Option<RecoveryOrderBlock>), PrepareLeaveAuthorityError> {
    let mut cursor = selected_major.checked_add(1);
    let mut movable_claims = Vec::new();
    let mut recovery = None;
    for unit in units {
        match unit {
            LeaveRelayUnit::Direct(mut claim) => {
                let Some(position) = cursor else {
                    return Err(PrepareLeaveAuthorityError::OrderCapacity);
                };
                claim.transaction_order = position;
                movable_claims.push(claim);
                cursor = position.checked_add(1);
            }
            LeaveRelayUnit::Recovery(block) => {
                let (relayed, next) = relay_recovery_block(block, cursor)?;
                recovery = Some(relayed);
                cursor = next;
            }
        }
    }
    movable_claims.sort_by_key(|claim| claim.transaction_order);
    Ok((movable_claims, recovery))
}

const fn relay_recovery_block(
    block: RecoveryOrderBlock,
    cursor: Option<TransactionOrder>,
) -> Result<(RecoveryOrderBlock, Option<TransactionOrder>), PrepareLeaveAuthorityError> {
    let mut next = cursor;
    let active_binding = if let Some(mut active) = block.active_binding {
        let Some(position) = next else {
            return Err(PrepareLeaveAuthorityError::OrderCapacity);
        };
        active.transaction_order = position;
        next = position.checked_add(1);
        Some(active)
    } else {
        None
    };
    let Some(recovery_operation_order) = next else {
        return Err(PrepareLeaveAuthorityError::OrderCapacity);
    };
    let Some(replacement_terminal_order) = recovery_operation_order.checked_add(1) else {
        return Err(PrepareLeaveAuthorityError::OrderCapacity);
    };
    Ok((
        RecoveryOrderBlock {
            active_binding,
            recovery_operation_order,
            replacement_terminal_order,
            participant_index: block.participant_index,
            marker_delivery_seq: block.marker_delivery_seq,
            recovered_binding_epoch: block.recovered_binding_epoch,
        },
        replacement_terminal_order.checked_add(1),
    ))
}

fn leave_resulting_order_ledger(
    selected_major: TransactionOrder,
    movable_claims: &[MovableOrderClaim],
    recovery: Option<RecoveryOrderBlock>,
) -> Result<OrderLedger, PrepareLeaveAuthorityError> {
    let active_binding_terminals =
        usize_to_u64(
            movable_claims
                .iter()
                .filter(|claim| matches!(claim.owner, OrderDirectOwner::ActiveBindingTerminal(_)))
                .count(),
        ) + u64::from(recovery.is_some_and(|block| block.active_binding.is_some()));
    let membership_exits = usize_to_u64(
        movable_claims
            .iter()
            .filter(|claim| matches!(claim.owner, OrderDirectOwner::MembershipExit { .. }))
            .count(),
    );
    let has_recovery = recovery.is_some();
    let resulting_claims = OrderClaims::new(
        active_binding_terminals,
        membership_exits,
        has_recovery,
        has_recovery,
    )
    .map_err(|_| PrepareLeaveAuthorityError::ResultingOrderLedger)?;
    OrderLedger::try_new(OrderHigh::Allocated(selected_major), resulting_claims)
        .map_err(|_| PrepareLeaveAuthorityError::ResultingOrderLedger)
}

fn consume_leave_order_lane(
    order: &mut OrderClaimFrontier,
    participant_id: ParticipantId,
    ended_binding_epoch: Option<BindingEpoch>,
    pending_order: Option<super::AdmissionOrder>,
) -> Result<TransactionOrder, PrepareLeaveAuthorityError> {
    let selection = select_leave_order(order, participant_id, ended_binding_epoch, pending_order)?;
    let (movable_claims, recovery) =
        relay_leave_order_units(selection.units, selection.selected_major)?;
    let ledger = leave_resulting_order_ledger(selection.selected_major, &movable_claims, recovery)?;
    *order = OrderClaimFrontier {
        ledger,
        movable_claims,
        immutable_candidates: Vec::new(),
        recovery,
    };
    Ok(selection.selected_major)
}

impl ClaimFrontiersPrevalidated {
    /// Returns the conversation whose raw closure edge must be restored.
    #[must_use]
    pub(super) const fn conversation_id(&self) -> ConversationId {
        self.conversation_id
    }

    /// Consumes the sole retained-marker authority needed by cold edge restore.
    ///
    /// A second request is refused even for the same sequence. The returned
    /// token is non-cloneable and binds conversation, record key, target
    /// participant, and exact current/last binding epoch.
    pub(super) fn take_marker_record(
        &mut self,
        request: MarkerRecordRequest,
    ) -> Option<ValidatedMarkerRecord> {
        if self.issued_marker_record.is_some() {
            return None;
        }
        let record = self
            .retained_records
            .iter()
            .find(|record| record.delivery_seq == request.marker_delivery_seq)
            .copied()?;
        let RetainedCausalRecordKind::CompactionMarker {
            participant_index,
            provenance,
        } = record.kind
        else {
            return None;
        };
        if participant_index != request.participant_index {
            return None;
        }
        let participant = active_participant(&self.active_identities, participant_index)?;
        let historical_delivery = self
            .historical_marker_deliveries
            .iter()
            .find(|authority| authority.marker_delivery_seq == request.marker_delivery_seq);
        let (target_binding, occurrence) = match request.use_kind {
            MarkerRecordUse::Planned(target) => {
                if participant.binding != target || historical_delivery.is_some() {
                    return None;
                }
                (target, MarkerRecordOccurrence::Undelivered)
            }
            MarkerRecordUse::Delivered(target) => {
                let delivered_binding_epoch = binding_epoch(target);
                if participant.binding != target
                    || !historical_delivery.is_some_and(|authority| {
                        authority.participant_index == participant_index
                            && authority.delivered_binding_epoch == delivered_binding_epoch
                    })
                {
                    return None;
                }
                (target, MarkerRecordOccurrence::Delivered)
            }
            MarkerRecordUse::Recovered {
                prior_binding_epoch,
                recovered_binding_epoch,
            } => {
                if participant.binding != FrontierBinding::Detached(recovered_binding_epoch)
                    || !historical_delivery.is_some_and(|authority| {
                        authority.participant_index == participant_index
                            && authority.delivered_binding_epoch == prior_binding_epoch
                    })
                {
                    return None;
                }
                (
                    FrontierBinding::Detached(prior_binding_epoch),
                    MarkerRecordOccurrence::Delivered,
                )
            }
        };
        self.issued_marker_record = Some(request);
        Some(ValidatedMarkerRecord {
            conversation_id: self.conversation_id,
            record,
            provenance,
            target_binding,
            occurrence,
            seal: MarkerAuthoritySeal::Validated,
        })
    }

    /// Completes logical-owner restoration after storage has rebuilt its exact
    /// typed closure edge from any sealed retained-marker token.
    pub(super) fn finish(
        self,
        edge: Option<super::StoredEdge>,
    ) -> Result<ClaimFrontiers, ParticipantStateCorruptReason> {
        self.validate_current_marker_edge(edge)?;
        self.validate_historical_delivery_consumers(edge)?;
        let recovery_provenance = self.resolve_recovery_provenance(edge)?;
        let sequence = restore_sequence_frontier(
            &self.active_identities,
            self.sequence_restore,
            self.retained_floor,
            self.sequence_ledger,
            recovery_provenance,
            &self.retained_records,
            &self.historical_causal_authorities,
        )
        .map_err(corrupt_frontier)?;
        let order = restore_order_frontier(
            &self.active_identities,
            self.order_restore,
            self.order_ledger,
            recovery_provenance,
        )
        .map_err(corrupt_frontier)?;
        validate_cross_counter(&sequence, &order).map_err(corrupt_frontier)?;
        Ok(ClaimFrontiers {
            conversation_id: self.conversation_id,
            active_identities: self.active_identities,
            identity_slot_limit: self.identity_slot_limit,
            retained_floor: self.retained_floor,
            retained_records: self.retained_records,
            marker_records: self.marker_records,
            sequence,
            order,
        })
    }

    fn validate_historical_delivery_consumers(
        &self,
        edge: Option<super::StoredEdge>,
    ) -> Result<(), ParticipantStateCorruptReason> {
        for history in &self.historical_marker_deliveries {
            let recovered_origin = self.binding_origins.iter().any(|origin| {
                origin.participant_id() == history.participant_index
                    && origin.recovered_marker()
                        == Some((history.marker_delivery_seq, history.delivered_binding_epoch))
            });
            let current_marker_edge = match edge {
                Some(super::StoredEdge::ParticipantCursorProgress(progress)) => {
                    progress.participant_id() == history.participant_index
                        && progress.marker_delivery_seq() == Some(history.marker_delivery_seq)
                        && progress.binding_epoch() == history.delivered_binding_epoch
                }
                Some(super::StoredEdge::DetachedCredentialRecovery(recovery)) => {
                    recovery.participant_id() == history.participant_index
                        && recovery.marker_delivery_seq() == history.marker_delivery_seq
                        && recovery.prior_binding_epoch() == history.delivered_binding_epoch
                }
                Some(
                    super::StoredEdge::ObserverProjection(_)
                    | super::StoredEdge::PhysicalCompaction(_)
                    | super::StoredEdge::MarkerDelivery(_)
                    | super::StoredEdge::DetachedMarkerRelease(_)
                    | super::StoredEdge::DetachedCursorRelease(_),
                )
                | None => false,
            };
            if !recovered_origin && !current_marker_edge {
                return Err(self.marker_corruption(history.marker_delivery_seq));
            }
        }
        Ok(())
    }

    fn validate_current_marker_edge(
        &self,
        edge: Option<super::StoredEdge>,
    ) -> Result<(), ParticipantStateCorruptReason> {
        if let Some(MarkerRecordRequest {
            participant_index,
            marker_delivery_seq,
            use_kind:
                MarkerRecordUse::Recovered {
                    recovered_binding_epoch,
                    ..
                },
        }) = self.issued_marker_record
        {
            let recovered_edge_matches = matches!(
                edge,
                Some(super::StoredEdge::DetachedCursorRelease(release))
                    if release.participant_id() == participant_index
                        && release.last_dead_binding_epoch() == recovered_binding_epoch
            );
            if !recovered_edge_matches {
                return Err(self.marker_corruption(marker_delivery_seq));
            }
            return Ok(());
        }
        let Some(context) = edge.and_then(marker_edge_context) else {
            return Ok(());
        };
        if self
            .issued_marker_record
            .is_none_or(|request| request.marker_delivery_seq != context.marker_delivery_seq)
            || !self.marker_context_matches(context)
        {
            return Err(self.marker_corruption(context.marker_delivery_seq));
        }
        Ok(())
    }

    fn resolve_recovery_provenance(
        &self,
        edge: Option<super::StoredEdge>,
    ) -> Result<Option<RecoveryClaimProvenance>, ParticipantStateCorruptReason> {
        let Some(marker_delivery_seq) = self.recovery_marker_delivery_seq else {
            return Ok(None);
        };

        if let Some(super::StoredEdge::DetachedCredentialRecovery(recovery)) = edge {
            return self
                .resolve_postfate_recovery_provenance(marker_delivery_seq, recovery)
                .map(Some);
        }

        let candidate =
            self.sequence_restore.immutable_candidates.iter().find_map(
                |candidate| match candidate {
                    ImmutableSequenceCandidate::Marker(marker)
                        if marker.delivery_seq == marker_delivery_seq
                            && matches!(marker.target_binding, FrontierBinding::Bound(_)) =>
                    {
                        Some(*marker)
                    }
                    _ => None,
                },
            );
        let retained = self.retained_records.iter().find_map(|record| {
            let RetainedCausalRecordKind::CompactionMarker {
                participant_index, ..
            } = record.kind
            else {
                return None;
            };
            if record.delivery_seq != marker_delivery_seq {
                return None;
            }
            let participant = active_participant(&self.active_identities, participant_index)?;
            let FrontierBinding::Bound(binding_epoch) = participant.binding else {
                return None;
            };
            let historical = self.historical_marker_deliveries.iter().find(|history| {
                history.participant_index == participant_index
                    && history.marker_delivery_seq == marker_delivery_seq
            })?;
            Some((
                participant_index,
                binding_epoch,
                historical.delivered_binding_epoch,
            ))
        });
        let provenance = match (candidate, retained) {
            (Some(marker), None) => RecoveryClaimProvenance {
                participant_index: marker.admission_order.participant_index(),
                marker_delivery_seq,
                prior_binding_epoch: binding_epoch(marker.target_binding),
                current_binding_epoch: binding_epoch(marker.target_binding),
                phase: RecoveryClaimPhase::PreFate,
            },
            (None, Some((participant_index, current_binding_epoch, prior_binding_epoch))) => {
                let phase = if current_binding_epoch == prior_binding_epoch {
                    let Some(context) = edge.and_then(marker_edge_context) else {
                        return Err(self.marker_corruption(marker_delivery_seq));
                    };
                    if context.marker_delivery_seq != marker_delivery_seq
                        || context.participant_index != participant_index
                        || context.binding_epoch != prior_binding_epoch
                        || context.target_binding != FrontierBinding::Bound(prior_binding_epoch)
                    {
                        return Err(self.marker_corruption(marker_delivery_seq));
                    }
                    RecoveryClaimPhase::PreFate
                } else {
                    let recovered_origin_matches = self.binding_origins.iter().any(|origin| {
                        origin.participant_id() == participant_index
                            && origin.binding_epoch() == current_binding_epoch
                            && origin.recovered_marker()
                                == Some((marker_delivery_seq, prior_binding_epoch))
                    });
                    if !recovered_origin_matches
                        || !matches!(
                            edge,
                            Some(
                                super::StoredEdge::ObserverProjection(_)
                                    | super::StoredEdge::PhysicalCompaction(_)
                            )
                        )
                    {
                        return Err(self.marker_corruption(marker_delivery_seq));
                    }
                    RecoveryClaimPhase::RecoveredBound
                };
                RecoveryClaimProvenance {
                    participant_index,
                    marker_delivery_seq,
                    prior_binding_epoch,
                    current_binding_epoch,
                    phase,
                }
            }
            (Some(_), Some(_)) | (None, None) => {
                return Err(self.marker_corruption(marker_delivery_seq));
            }
        };
        Ok(Some(provenance))
    }

    fn resolve_postfate_recovery_provenance(
        &self,
        marker_delivery_seq: DeliverySeq,
        recovery: super::DetachedCredentialRecovery,
    ) -> Result<RecoveryClaimProvenance, ParticipantStateCorruptReason> {
        let provenance = RecoveryClaimProvenance {
            participant_index: recovery.participant_id(),
            marker_delivery_seq: recovery.marker_delivery_seq(),
            prior_binding_epoch: recovery.prior_binding_epoch(),
            current_binding_epoch: recovery.prior_binding_epoch(),
            phase: RecoveryClaimPhase::PostFate,
        };
        let context = MarkerEdgeContext {
            participant_index: provenance.participant_index,
            marker_delivery_seq: provenance.marker_delivery_seq,
            binding_epoch: provenance.prior_binding_epoch,
            target_binding: FrontierBinding::Detached(provenance.prior_binding_epoch),
        };
        if marker_delivery_seq != provenance.marker_delivery_seq
            || self
                .issued_marker_record
                .is_none_or(|request| request.marker_delivery_seq != marker_delivery_seq)
            || !self.marker_context_matches(context)
        {
            return Err(self.marker_corruption(marker_delivery_seq));
        }
        Ok(provenance)
    }

    fn marker_context_matches(&self, context: MarkerEdgeContext) -> bool {
        self.marker_records.iter().any(|record| {
            matches!(
                record.kind,
                RetainedCausalRecordKind::CompactionMarker {
                    participant_index,
                    ..
                } if participant_index == context.participant_index
            ) && record.delivery_seq == context.marker_delivery_seq
                && active_participant(&self.active_identities, context.participant_index)
                    .is_some_and(|participant| participant.binding == context.target_binding)
        })
    }

    fn marker_corruption(&self, delivery_seq: DeliverySeq) -> ParticipantStateCorruptReason {
        corrupt_frontier(sequence_error(
            sequence_ordinal(self.sequence_ledger, delivery_seq),
            ClaimFrontierInvalidReason::RecoveryBlock,
        ))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MarkerEdgeContext {
    participant_index: ParticipantId,
    marker_delivery_seq: DeliverySeq,
    binding_epoch: BindingEpoch,
    target_binding: FrontierBinding,
}

fn marker_edge_context(edge: super::StoredEdge) -> Option<MarkerEdgeContext> {
    match edge {
        super::StoredEdge::MarkerDelivery(delivery) => Some(MarkerEdgeContext {
            participant_index: delivery.participant_id(),
            marker_delivery_seq: delivery.marker_delivery_seq(),
            binding_epoch: delivery.binding_epoch(),
            target_binding: FrontierBinding::Bound(delivery.binding_epoch()),
        }),
        super::StoredEdge::ParticipantCursorProgress(progress) => {
            let marker_delivery_seq = progress.marker_delivery_seq()?;
            Some(MarkerEdgeContext {
                participant_index: progress.participant_id(),
                marker_delivery_seq,
                binding_epoch: progress.binding_epoch(),
                target_binding: FrontierBinding::Bound(progress.binding_epoch()),
            })
        }
        super::StoredEdge::DetachedCredentialRecovery(recovery) => Some(MarkerEdgeContext {
            participant_index: recovery.participant_id(),
            marker_delivery_seq: recovery.marker_delivery_seq(),
            binding_epoch: recovery.prior_binding_epoch(),
            target_binding: FrontierBinding::Detached(recovery.prior_binding_epoch()),
        }),
        super::StoredEdge::DetachedMarkerRelease(release) => Some(MarkerEdgeContext {
            participant_index: release.participant_id(),
            marker_delivery_seq: release.marker_delivery_seq(),
            binding_epoch: release.last_dead_binding_epoch(),
            target_binding: FrontierBinding::Detached(release.last_dead_binding_epoch()),
        }),
        super::StoredEdge::ObserverProjection(_)
        | super::StoredEdge::PhysicalCompaction(_)
        | super::StoredEdge::DetachedCursorRelease(_) => None,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum SequenceClass {
    Exit = 0,
    Terminal = 1,
    Marker = 2,
    RecoveryAttach = 3,
    RecoveryReplacementTerminal = 4,
    LiveTimesTerminal = 5,
    LiveTimesReplacementTerminal = 6,
    OtherLiveTimesExit = 7,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum OrderClass {
    ActiveBindingTerminal = 0,
    MembershipExit = 1,
    RecoveryOperation = 2,
    RecoveryReplacementTerminal = 3,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct NumericSegment<C> {
    start: u128,
    length: u128,
    class: Option<C>,
    immutable: bool,
}

fn first_duplicate_candidate_key(
    candidates: &[ImmutableSequenceCandidate],
    retained_records: &[RetainedCausalRecord],
) -> Option<super::AdmissionOrder> {
    let mut keys: Vec<_> = candidates
        .iter()
        .map(|candidate| candidate.admission_order())
        .chain(retained_records.iter().map(|record| record.admission_order))
        .collect();
    keys.sort_unstable();
    let mut previous = None;
    for key in keys {
        if previous == Some(key) {
            return Some(key);
        }
        previous = Some(key);
    }
    None
}

fn validate_unique_candidate_keys(
    candidates: &[ImmutableSequenceCandidate],
    retained_records: &[RetainedCausalRecord],
) -> Result<(), ParticipantStateCorruptReason> {
    let Some(order) = first_duplicate_candidate_key(candidates, retained_records) else {
        return Ok(());
    };
    Err(ParticipantStateCorruptReason::DuplicateCandidateKey {
        transaction_order: order.transaction_order(),
        candidate_phase: order.candidate_phase(),
        participant_index: order.participant_index(),
    })
}

fn validate_sequence_numeric(
    restore: &SequenceClaimFrontierRestore,
    ledger: SequenceLedger,
) -> Result<(), ClaimFrontierError> {
    let mut segments = sequence_segments(restore);
    validate_numeric_segments(
        ClaimFrontierCounter::DeliverySequence,
        u128::from(ledger.high_watermark()) + 1,
        ledger.required_reserve(),
        &mut segments,
        &sequence_expected_counts(ledger),
    )
}

fn validate_order_numeric(
    restore: &OrderClaimFrontierRestore,
    ledger: OrderLedger,
) -> Result<(), ClaimFrontierError> {
    let mut segments = order_segments(restore, ledger.high());
    validate_numeric_segments(
        ClaimFrontierCounter::TransactionOrder,
        order_frontier_start(ledger.high()),
        order_frontier_candidate_count(restore, ledger.high()) + ledger.claims().total(),
        &mut segments,
        &order_expected_counts(ledger),
    )
}

fn validate_bounded_shape(
    restore: &ClaimFrontiersRestore,
    sequence_ledger: SequenceLedger,
) -> Result<(), ClaimFrontierError> {
    let identity_limit = u128::from(restore.identity_slot_limit);
    let twice_identity_limit = identity_limit.saturating_mul(2);
    let order_candidate_keys = restore
        .order
        .immutable_candidates
        .iter()
        .fold(0_u128, |count, candidate| {
            count.saturating_add(usize_to_u128(candidate.candidate_keys.len()))
        });
    let bounded = usize_to_u128(restore.active_identities.len()) <= identity_limit
        && usize_to_u128(restore.sequence.movable_claims.len()) <= twice_identity_limit
        && usize_to_u128(restore.sequence.immutable_candidates.len()) <= twice_identity_limit
        && usize_to_u128(restore.sequence.products.live_times_terminal.len()) <= identity_limit
        && usize_to_u128(restore.sequence.products.other_live_times_exit.len()) <= identity_limit
        && usize_to_u128(restore.historical_marker_deliveries.len())
            <= u128::from(restore.retained_record_limit)
        && usize_to_u128(restore.historical_causal_facts.len()) <= twice_identity_limit
        && usize_to_u128(restore.order.movable_claims.len()) <= twice_identity_limit
        && usize_to_u128(restore.order.immutable_candidates.len()) <= twice_identity_limit
        && order_candidate_keys <= twice_identity_limit;
    if bounded {
        Ok(())
    } else {
        Err(sequence_error(
            sequence_ledger.required_reserve(),
            ClaimFrontierInvalidReason::LogicalOwner,
        ))
    }
}

fn validated_retained_records(
    mut records: Vec<RetainedCausalRecord>,
    retained_floor: u128,
    retained_record_limit: u64,
    identity_slot_limit: u64,
    ledger: SequenceLedger,
) -> Result<Vec<RetainedCausalRecord>, ClaimFrontierError> {
    let high_end = u128::from(ledger.high_watermark()) + 1;
    let expected_count = high_end.saturating_sub(retained_floor);
    if retained_floor > high_end
        || usize_to_u128(records.len()) > u128::from(retained_record_limit)
        || usize_to_u128(records.len()) != expected_count
    {
        return Err(sequence_error(
            usize_to_u128(records.len()).min(expected_count),
            ClaimFrontierInvalidReason::LogicalOwner,
        ));
    }
    records.sort_by_key(|record| record.delivery_seq);
    let mut previous_admission_order = None;
    for (record_index, record) in records.iter().enumerate() {
        let (participant_index, valid_kind) = match record.kind {
            RetainedCausalRecordKind::BindingTerminal(owner) => (
                owner.participant_index,
                record.admission_order.candidate_phase() == CandidatePhase::BindingTerminal
                    && record.admission_order.participant_index() == owner.participant_index,
            ),
            RetainedCausalRecordKind::MembershipExit { participant_index } => (
                participant_index,
                record.admission_order.candidate_phase() == CandidatePhase::MembershipExit
                    && record.admission_order.participant_index() == participant_index,
            ),
            RetainedCausalRecordKind::AttachLifecycle {
                participant_index, ..
            } => (
                participant_index,
                record.admission_order.candidate_phase() == CandidatePhase::AttachLifecycle
                    && record.admission_order.participant_index() == participant_index,
            ),
            RetainedCausalRecordKind::OrdinaryRecord { participant_index } => (
                participant_index,
                record.admission_order.candidate_phase() == CandidatePhase::OrdinaryRecord
                    && record.admission_order.participant_index() == participant_index,
            ),
            RetainedCausalRecordKind::CompactionMarker {
                participant_index,
                provenance,
            } => (
                participant_index,
                record.admission_order.candidate_phase() == CandidatePhase::CompactionMarker
                    && record.admission_order.participant_index() == participant_index
                    && marker_provenance_targets(provenance, participant_index),
            ),
        };
        let expected_sequence = retained_floor + rank_index(record_index);
        if participant_index >= identity_slot_limit
            || !valid_kind
            || previous_admission_order.is_some_and(|previous| previous >= record.admission_order)
            || u128::from(record.delivery_seq) != expected_sequence
        {
            return Err(sequence_error(
                rank_index(record_index),
                ClaimFrontierInvalidReason::CandidateKey,
            ));
        }
        previous_admission_order = Some(record.admission_order);
    }
    Ok(records)
}

fn validated_active_marker_records(
    retained_markers: &[RetainedCausalRecord],
    mut active_marker_anchors: Vec<DeliverySeq>,
    identity_slot_limit: u64,
    ledger: SequenceLedger,
) -> Result<Vec<RetainedCausalRecord>, ClaimFrontierError> {
    if usize_to_u128(active_marker_anchors.len()) > u128::from(identity_slot_limit) {
        return Err(sequence_error(
            ledger.required_reserve(),
            ClaimFrontierInvalidReason::LogicalOwner,
        ));
    }
    active_marker_anchors.sort_unstable();
    let mut previous_sequence = None;
    let mut owners = Vec::new();
    let mut active_records = Vec::new();
    for delivery_seq in active_marker_anchors {
        let Some(record) = retained_markers
            .iter()
            .find(|record| record.delivery_seq == delivery_seq)
            .copied()
        else {
            return Err(sequence_error(
                sequence_ordinal(ledger, delivery_seq),
                ClaimFrontierInvalidReason::LogicalOwner,
            ));
        };
        let RetainedCausalRecordKind::CompactionMarker {
            participant_index, ..
        } = record.kind
        else {
            return Err(sequence_error(
                sequence_ordinal(ledger, delivery_seq),
                ClaimFrontierInvalidReason::LogicalOwner,
            ));
        };
        if previous_sequence == Some(delivery_seq) || owners.contains(&participant_index) {
            return Err(sequence_error(
                sequence_ordinal(ledger, delivery_seq),
                ClaimFrontierInvalidReason::LogicalOwner,
            ));
        }
        previous_sequence = Some(delivery_seq);
        owners.push(participant_index);
        active_records.push(record);
    }
    Ok(active_records)
}

fn validated_historical_marker_deliveries(
    mut facts: Vec<HistoricalMarkerDeliveryFactRestore>,
    conversation_id: ConversationId,
    active: &ActiveIdentityRanks,
    retained_records: &[RetainedCausalRecord],
    historical_causal_authorities: &[HistoricalCausalAuthority],
    retained_record_limit: u64,
    ledger: SequenceLedger,
) -> Result<Vec<HistoricalMarkerDeliveryAuthority>, ClaimFrontierError> {
    if usize_to_u128(facts.len()) > u128::from(retained_record_limit) {
        return Err(sequence_error(
            ledger.required_reserve(),
            ClaimFrontierInvalidReason::LogicalOwner,
        ));
    }
    facts.sort_by_key(|fact| fact.marker_delivery_seq);
    let mut previous_sequence = None;
    let mut authorities = Vec::new();
    for fact in facts {
        let matching_record = retained_records.iter().find(|record| {
            record.delivery_seq == fact.marker_delivery_seq
                && matches!(
                    record.kind,
                    RetainedCausalRecordKind::CompactionMarker {
                        participant_index,
                        ..
                    } if participant_index == fact.participant_index
                )
        });
        let current_bound =
            active_participant(active, fact.participant_index).is_some_and(|participant| {
                participant.binding == FrontierBinding::Bound(fact.delivered_binding_epoch)
            });
        let terminal_order = historical_causal_authorities
            .iter()
            .find_map(|authority| match authority.kind {
                HistoricalCausalKind::BindingTerminal(owner)
                    if binding_terminal_matches_delivery(owner, fact) =>
                {
                    Some(authority.admission_order)
                }
                HistoricalCausalKind::BindingTerminal(_)
                | HistoricalCausalKind::MembershipExit(_) => None,
            })
            .or_else(|| {
                retained_records
                    .iter()
                    .find_map(|record| match record.kind {
                        RetainedCausalRecordKind::BindingTerminal(owner)
                            if binding_terminal_matches_delivery(owner, fact) =>
                        {
                            Some(record.admission_order)
                        }
                        RetainedCausalRecordKind::BindingTerminal(_)
                        | RetainedCausalRecordKind::MembershipExit { .. }
                        | RetainedCausalRecordKind::AttachLifecycle { .. }
                        | RetainedCausalRecordKind::OrdinaryRecord { .. }
                        | RetainedCausalRecordKind::CompactionMarker { .. } => None,
                    })
            });
        let historical_epoch_is_backed = matching_record.is_some_and(|marker_record| {
            current_bound
                || terminal_order
                    .is_some_and(|terminal_order| terminal_order > marker_record.admission_order)
        });
        if fact.conversation_id != conversation_id
            || previous_sequence == Some(fact.marker_delivery_seq)
            || !historical_epoch_is_backed
        {
            return Err(sequence_error(
                sequence_ordinal(ledger, fact.marker_delivery_seq),
                ClaimFrontierInvalidReason::LogicalOwner,
            ));
        }
        previous_sequence = Some(fact.marker_delivery_seq);
        authorities.push(HistoricalMarkerDeliveryAuthority {
            participant_index: fact.participant_index,
            marker_delivery_seq: fact.marker_delivery_seq,
            delivered_binding_epoch: fact.delivered_binding_epoch,
        });
    }
    Ok(authorities)
}

fn binding_terminal_matches_delivery(
    owner: BindingTerminalOwner,
    fact: HistoricalMarkerDeliveryFactRestore,
) -> bool {
    (owner.participant_index, owner.binding_epoch)
        == (fact.participant_index, fact.delivered_binding_epoch)
}

struct BindingOriginValidation<'a> {
    conversation_id: ConversationId,
    active: &'a ActiveIdentityRanks,
    origins: &'a [BindingOrigin],
    retained_records: &'a [RetainedCausalRecord],
    causal_authorities: &'a [HistoricalCausalAuthority],
    historical_marker_deliveries: &'a [HistoricalMarkerDeliveryAuthority],
    total: bool,
    ledger: SequenceLedger,
}

impl BindingOriginValidation<'_> {
    fn validate(&self) -> Result<(), ClaimFrontierError> {
        if !self.total {
            return if self.origins.is_empty() {
                Ok(())
            } else {
                Err(self.logical_owner_error())
            };
        }
        if self.origins.len() != self.active.participants.len() {
            return Err(self.logical_owner_error());
        }
        for participant in &self.active.participants {
            let mut matching = self
                .origins
                .iter()
                .filter(|origin| origin.participant_id() == participant.participant_index);
            let Some(origin) = matching.next() else {
                return Err(self.logical_owner_error());
            };
            if matching.next().is_some() {
                return Err(self.logical_owner_error());
            }
            self.validate_origin(*participant, *origin)?;
        }
        Ok(())
    }

    fn validate_origin(
        &self,
        participant: FrontierParticipant,
        origin: BindingOrigin,
    ) -> Result<(), ClaimFrontierError> {
        let current_epoch = binding_epoch(participant.binding);
        let attached = origin.attached();
        if origin.conversation_id() != self.conversation_id
            || origin.binding_epoch() != current_epoch
            || attached.conversation_id() != self.conversation_id
            || attached.participant_id() != participant.participant_index
            || attached.binding_epoch() != current_epoch
            || attached.admission_order().candidate_phase() != CandidatePhase::AttachLifecycle
        {
            return Err(self.logical_owner_error());
        }
        let mut retained_attach_for_binding = self.retained_records.iter().filter(|record| {
            matches!(
                record.kind,
                RetainedCausalRecordKind::AttachLifecycle {
                    participant_index,
                    binding_epoch,
                } if participant_index == participant.participant_index
                    && binding_epoch == current_epoch
            )
        });
        let retained_attach_matches = retained_attach_for_binding.clone().any(|record| {
            record.delivery_seq == attached.delivery_seq()
                && record.admission_order == attached.admission_order()
        });
        if retained_attach_for_binding.next().is_some() && !retained_attach_matches {
            return Err(self.logical_owner_error());
        }
        if let Some((marker_delivery_seq, prior_binding_epoch)) = origin.recovered_marker() {
            let generation_is_next = prior_binding_epoch
                .capability_generation
                .get()
                .checked_add(1)
                == Some(current_epoch.capability_generation.get());
            let marker_history_matches = self.historical_marker_deliveries.iter().any(|history| {
                history.participant_index == participant.participant_index
                    && history.marker_delivery_seq == marker_delivery_seq
                    && history.delivered_binding_epoch == prior_binding_epoch
            });
            if !generation_is_next || !marker_history_matches {
                return Err(sequence_error(
                    sequence_ordinal(self.ledger, marker_delivery_seq),
                    ClaimFrontierInvalidReason::RecoveryBlock,
                ));
            }
        } else if matches!(participant.binding, FrontierBinding::Detached(_))
            && !binding_terminal_exists(
                participant.participant_index,
                current_epoch,
                self.retained_records,
                self.causal_authorities,
            )
        {
            return Err(self.logical_owner_error());
        }
        Ok(())
    }

    const fn logical_owner_error(&self) -> ClaimFrontierError {
        sequence_error(
            self.ledger.required_reserve(),
            ClaimFrontierInvalidReason::LogicalOwner,
        )
    }
}

fn binding_terminal_exists(
    participant_index: ParticipantId,
    binding_epoch: BindingEpoch,
    retained_records: &[RetainedCausalRecord],
    historical_causal_authorities: &[HistoricalCausalAuthority],
) -> bool {
    retained_records.iter().any(|record| {
        matches!(
            record.kind,
            RetainedCausalRecordKind::BindingTerminal(owner)
                if owner.participant_index == participant_index
                    && owner.binding_epoch == binding_epoch
        )
    }) || historical_causal_authorities.iter().any(|authority| {
        matches!(
            authority.kind,
            HistoricalCausalKind::BindingTerminal(owner)
                if owner.participant_index == participant_index
                    && owner.binding_epoch == binding_epoch
        )
    })
}

fn validate_marker_credit_owners(
    candidates: &[ImmutableSequenceCandidate],
    marker_records: &[RetainedCausalRecord],
    identity_slot_limit: u64,
    ledger: SequenceLedger,
) -> Result<(), ClaimFrontierError> {
    let mut owners = Vec::new();
    for record in marker_records {
        let RetainedCausalRecordKind::CompactionMarker {
            participant_index, ..
        } = record.kind
        else {
            continue;
        };
        if owners.contains(&participant_index) {
            return Err(sequence_error(
                ledger.required_reserve(),
                ClaimFrontierInvalidReason::LogicalOwner,
            ));
        }
        owners.push(participant_index);
    }
    for candidate in candidates {
        let ImmutableSequenceCandidate::Marker(marker) = candidate else {
            continue;
        };
        let participant_index = marker.admission_order.participant_index();
        if owners.contains(&participant_index) {
            return Err(sequence_error(
                sequence_ordinal(ledger, marker.delivery_seq),
                ClaimFrontierInvalidReason::LogicalOwner,
            ));
        }
        owners.push(participant_index);
    }
    if usize_to_u128(owners.len()) > u128::from(identity_slot_limit) {
        return Err(sequence_error(
            ledger.required_reserve(),
            ClaimFrontierInvalidReason::LogicalOwner,
        ));
    }
    Ok(())
}

fn validated_historical_authorities(
    facts: Vec<HistoricalCausalFactRestore>,
    conversation_id: ConversationId,
    identity_slot_limit: u64,
    ledger: SequenceLedger,
    history: &ValidatedConversationHistory,
) -> Result<Vec<HistoricalCausalAuthority>, ClaimFrontierError> {
    if usize_to_u128(facts.len()) > u128::from(identity_slot_limit).saturating_mul(2) {
        return Err(sequence_error(
            ledger.required_reserve(),
            ClaimFrontierInvalidReason::LogicalOwner,
        ));
    }
    let authorities: Vec<_> = facts
        .into_iter()
        .map(HistoricalCausalAuthority::from_restore)
        .collect();
    let mut seen = Vec::new();
    for authority in &authorities {
        let (participant_index, phase) = match authority.kind {
            HistoricalCausalKind::BindingTerminal(owner) => {
                (owner.participant_index, CandidatePhase::BindingTerminal)
            }
            HistoricalCausalKind::MembershipExit(participant_index) => {
                (participant_index, CandidatePhase::MembershipExit)
            }
        };
        if authority.conversation_id != conversation_id
            || participant_index >= identity_slot_limit
            || authority.admission_order.participant_index() != participant_index
            || authority.admission_order.candidate_phase() != phase
            || seen.contains(authority)
            || !history.causal_authorities.contains(authority)
        {
            return Err(sequence_error(
                ledger.required_reserve(),
                ClaimFrontierInvalidReason::LogicalOwner,
            ));
        }
        seen.push(*authority);
    }
    Ok(authorities)
}

const fn corrupt_frontier(error: ClaimFrontierError) -> ParticipantStateCorruptReason {
    ParticipantStateCorruptReason::ClaimFrontierInvalid {
        counter: match error.counter {
            ClaimFrontierCounter::DeliverySequence => ClaimCounter::DeliverySeq,
            ClaimFrontierCounter::TransactionOrder => ClaimCounter::TransactionOrder,
        },
        first_bad_position: error.first_bad_position,
    }
}

fn sequence_segments(restore: &SequenceClaimFrontierRestore) -> Vec<NumericSegment<SequenceClass>> {
    let mut segments = Vec::new();
    for claim in &restore.movable_claims {
        segments.push(NumericSegment {
            start: u128::from(claim.delivery_seq),
            length: 1,
            class: Some(match claim.owner {
                SequenceDirectOwner::MembershipExit { .. } => SequenceClass::Exit,
                SequenceDirectOwner::BindingTerminal(_) => SequenceClass::Terminal,
            }),
            immutable: false,
        });
    }
    for candidate in &restore.immutable_candidates {
        segments.push(NumericSegment {
            start: u128::from(candidate.delivery_seq()),
            length: 1,
            class: Some(sequence_candidate_class(*candidate)),
            immutable: true,
        });
    }
    for range in &restore.products.live_times_terminal {
        segments.push(NumericSegment {
            start: u128::from(range.start),
            length: u128::from(range.length),
            class: Some(SequenceClass::LiveTimesTerminal),
            immutable: false,
        });
    }
    if let Some(range) = restore.products.live_times_replacement_terminal {
        segments.push(NumericSegment {
            start: u128::from(range.start),
            length: u128::from(range.length),
            class: Some(SequenceClass::LiveTimesReplacementTerminal),
            immutable: false,
        });
    }
    for range in &restore.products.other_live_times_exit {
        segments.push(NumericSegment {
            start: u128::from(range.start),
            length: u128::from(range.length),
            class: Some(SequenceClass::OtherLiveTimesExit),
            immutable: false,
        });
    }
    if let Some(recovery) = restore.recovery {
        if let Some(terminal) = recovery.terminal {
            segments.push(NumericSegment {
                start: u128::from(terminal.delivery_seq),
                length: 1,
                class: Some(SequenceClass::Terminal),
                immutable: false,
            });
        }
        segments.push(NumericSegment {
            start: u128::from(recovery.recovery_attach_seq),
            length: 1,
            class: Some(SequenceClass::RecoveryAttach),
            immutable: false,
        });
        segments.push(NumericSegment {
            start: u128::from(recovery.replacement_terminal_seq),
            length: 1,
            class: Some(SequenceClass::RecoveryReplacementTerminal),
            immutable: false,
        });
    }
    segments
}

fn order_segments(
    restore: &OrderClaimFrontierRestore,
    high: OrderHigh,
) -> Vec<NumericSegment<OrderClass>> {
    let mut segments = Vec::new();
    for claim in &restore.movable_claims {
        segments.push(NumericSegment {
            start: u128::from(claim.transaction_order),
            length: 1,
            class: Some(match claim.owner {
                OrderDirectOwner::ActiveBindingTerminal(_) => OrderClass::ActiveBindingTerminal,
                OrderDirectOwner::MembershipExit { .. } => OrderClass::MembershipExit,
            }),
            immutable: false,
        });
    }
    for candidate in restore
        .immutable_candidates
        .iter()
        .filter(|candidate| order_is_above_high(candidate.transaction_order, high))
    {
        segments.push(NumericSegment {
            start: u128::from(candidate.transaction_order),
            length: 1,
            class: None,
            immutable: true,
        });
    }
    if let Some(recovery) = restore.recovery {
        if let Some(active_binding) = recovery.active_binding {
            segments.push(NumericSegment {
                start: u128::from(active_binding.transaction_order),
                length: 1,
                class: Some(OrderClass::ActiveBindingTerminal),
                immutable: false,
            });
        }
        segments.push(NumericSegment {
            start: u128::from(recovery.recovery_operation_order),
            length: 1,
            class: Some(OrderClass::RecoveryOperation),
            immutable: false,
        });
        segments.push(NumericSegment {
            start: u128::from(recovery.replacement_terminal_order),
            length: 1,
            class: Some(OrderClass::RecoveryReplacementTerminal),
            immutable: false,
        });
    }
    segments
}

fn restore_sequence_frontier(
    active: &ActiveIdentityRanks,
    restore: SequenceClaimFrontierRestore,
    retained_floor: u128,
    ledger: SequenceLedger,
    recovery_provenance: Option<RecoveryClaimProvenance>,
    retained_records: &[RetainedCausalRecord],
    historical_causal_authorities: &[HistoricalCausalAuthority],
) -> Result<SequenceClaimFrontier, ClaimFrontierError> {
    let mut segments = sequence_segments(&restore);

    let expected_counts = sequence_expected_counts(ledger);
    validate_numeric_segments(
        ClaimFrontierCounter::DeliverySequence,
        u128::from(ledger.high_watermark()) + 1,
        ledger.required_reserve(),
        &mut segments,
        &expected_counts,
    )?;
    validate_sequence_recovery(
        active,
        restore.recovery,
        recovery_provenance,
        ledger,
        ledger.required_reserve(),
    )?;
    validate_sequence_candidates(
        active,
        &restore.immutable_candidates,
        retained_floor,
        retained_records,
        historical_causal_authorities,
        ledger,
    )?;
    let terminal_owners = validate_sequence_direct_owners(
        active,
        &restore.movable_claims,
        &restore.immutable_candidates,
        restore.recovery,
        ledger,
    )?;
    let products = validate_sequence_products(
        active,
        restore.products,
        &terminal_owners,
        restore.recovery,
        recovery_provenance,
        ledger,
    )?;
    let recovery = restore
        .recovery
        .zip(recovery_provenance)
        .map(|(value, provenance)| RecoverySequenceBlock {
            terminal: value.terminal,
            recovery_attach_seq: value.recovery_attach_seq,
            replacement_terminal_seq: value.replacement_terminal_seq,
            participant_index: provenance.participant_index,
            marker_delivery_seq: provenance.marker_delivery_seq,
            recovered_binding_epoch: provenance.prior_binding_epoch,
        });

    let mut movable_claims = restore.movable_claims;
    movable_claims.sort_by_key(|claim| claim.delivery_seq);
    let mut immutable_candidates = restore.immutable_candidates;
    immutable_candidates.sort_by_key(|candidate| candidate.delivery_seq());

    Ok(SequenceClaimFrontier {
        ledger,
        movable_claims,
        immutable_candidates,
        products,
        recovery,
    })
}

fn restore_order_frontier(
    active: &ActiveIdentityRanks,
    restore: OrderClaimFrontierRestore,
    ledger: OrderLedger,
    recovery_provenance: Option<RecoveryClaimProvenance>,
) -> Result<OrderClaimFrontier, ClaimFrontierError> {
    let mut segments = order_segments(&restore, ledger.high());

    let candidate_count = order_frontier_candidate_count(&restore, ledger.high());
    let expected_length = candidate_count + ledger.claims().total();
    let expected_counts = order_expected_counts(ledger);
    validate_numeric_segments(
        ClaimFrontierCounter::TransactionOrder,
        order_frontier_start(ledger.high()),
        expected_length,
        &mut segments,
        &expected_counts,
    )?;
    validate_order_recovery(
        active,
        restore.recovery,
        recovery_provenance,
        ledger,
        expected_length,
    )?;
    let immutable_candidates = validate_order_candidates(&restore.immutable_candidates, ledger)?;
    validate_order_direct_owners(active, &restore.movable_claims, restore.recovery, ledger)?;
    let recovery = restore
        .recovery
        .zip(recovery_provenance)
        .map(|(value, provenance)| RecoveryOrderBlock {
            active_binding: value.active_binding,
            recovery_operation_order: value.recovery_operation_order,
            replacement_terminal_order: value.replacement_terminal_order,
            participant_index: provenance.participant_index,
            marker_delivery_seq: provenance.marker_delivery_seq,
            recovered_binding_epoch: provenance.prior_binding_epoch,
        });

    let mut movable_claims = restore.movable_claims;
    movable_claims.sort_by_key(|claim| claim.transaction_order);

    Ok(OrderClaimFrontier {
        ledger,
        movable_claims,
        immutable_candidates,
        recovery,
    })
}

fn validate_numeric_segments<C: Copy + Into<usize>>(
    counter: ClaimFrontierCounter,
    first_value: u128,
    expected_length: u128,
    segments: &mut [NumericSegment<C>],
    expected_counts: &[u128],
) -> Result<(), ClaimFrontierError> {
    segments.sort_by_key(|segment| segment.start);
    let mut events = numeric_events(counter, first_value, segments)?;
    let emitted = scan_numeric_events(counter, first_value, &mut events)?;
    if emitted != expected_length {
        return Err(frontier_error(
            counter,
            emitted.min(expected_length),
            ClaimFrontierInvalidReason::AggregateLedger,
        ));
    }
    validate_immutable_prefix(counter, first_value, segments)?;
    validate_segment_class_counts(counter, expected_length, segments, expected_counts)
}

fn numeric_events<C>(
    counter: ClaimFrontierCounter,
    first_value: u128,
    segments: &[NumericSegment<C>],
) -> Result<Vec<(u128, i8)>, ClaimFrontierError> {
    let mut events = Vec::new();
    let counter_limit = u128::from(u64::MAX) + 1;
    for segment in segments {
        if segment.length == 0 {
            continue;
        }
        let Some(end) = segment.start.checked_add(segment.length) else {
            return Err(frontier_error(
                counter,
                counter_limit.saturating_sub(first_value),
                ClaimFrontierInvalidReason::NumericPosition,
            ));
        };
        if segment.start < first_value {
            return Err(frontier_error(
                counter,
                0,
                ClaimFrontierInvalidReason::NumericPosition,
            ));
        }
        if end > counter_limit {
            return Err(frontier_error(
                counter,
                counter_limit.saturating_sub(first_value),
                ClaimFrontierInvalidReason::NumericPosition,
            ));
        }
        events.push((segment.start, 1_i8));
        events.push((end, -1_i8));
    }
    Ok(events)
}

fn scan_numeric_events(
    counter: ClaimFrontierCounter,
    first_value: u128,
    events: &mut [(u128, i8)],
) -> Result<u128, ClaimFrontierError> {
    events.sort_unstable_by_key(|event| event.0);
    let mut event_index = 0_usize;
    let mut coordinate = first_value;
    let mut coverage = 0_i128;
    let mut emitted = 0_u128;
    while let Some((event_coordinate, _)) = events.get(event_index).copied() {
        if event_coordinate > coordinate {
            if coverage == 0 {
                return Err(frontier_error(
                    counter,
                    emitted,
                    ClaimFrontierInvalidReason::NumericPosition,
                ));
            }
            if coverage > 1 {
                return Err(frontier_error(
                    counter,
                    emitted.saturating_add(1),
                    ClaimFrontierInvalidReason::NumericPosition,
                ));
            }
            emitted = emitted.saturating_add(event_coordinate - coordinate);
            coordinate = event_coordinate;
        }
        while let Some((same_coordinate, delta)) = events.get(event_index).copied() {
            if same_coordinate != coordinate {
                break;
            }
            coverage += i128::from(delta);
            event_index += 1;
        }
    }
    if coverage != 0 {
        return Err(frontier_error(
            counter,
            emitted,
            ClaimFrontierInvalidReason::NumericPosition,
        ));
    }
    Ok(emitted)
}

fn validate_immutable_prefix<C>(
    counter: ClaimFrontierCounter,
    first_value: u128,
    segments: &[NumericSegment<C>],
) -> Result<(), ClaimFrontierError> {
    let mut first_movable = None;
    for segment in segments.iter().filter(|segment| segment.length != 0) {
        if segment.immutable {
            if let Some(first_movable) = first_movable {
                return Err(frontier_error(
                    counter,
                    first_movable,
                    ClaimFrontierInvalidReason::NumericPosition,
                ));
            }
        } else if first_movable.is_none() {
            first_movable = Some(segment.start - first_value);
        }
    }
    Ok(())
}

fn validate_segment_class_counts<C: Copy + Into<usize>>(
    counter: ClaimFrontierCounter,
    expected_length: u128,
    segments: &[NumericSegment<C>],
    expected_counts: &[u128],
) -> Result<(), ClaimFrontierError> {
    let mut actual_counts = core::iter::repeat_n(0_u128, expected_counts.len()).collect::<Vec<_>>();
    let mut class_ordinal = 0_u128;
    for segment in segments {
        if segment.length == 0 {
            continue;
        }
        if let Some(class) = segment.class {
            let index = class.into();
            let prior = actual_counts[index];
            let Some(resulting) = prior.checked_add(segment.length) else {
                return Err(frontier_error(
                    counter,
                    class_ordinal,
                    ClaimFrontierInvalidReason::AggregateLedger,
                ));
            };
            if resulting > expected_counts[index] {
                return Err(frontier_error(
                    counter,
                    class_ordinal + expected_counts[index].saturating_sub(prior),
                    ClaimFrontierInvalidReason::AggregateLedger,
                ));
            }
            actual_counts[index] = resulting;
        }
        class_ordinal += segment.length;
    }
    if actual_counts != expected_counts {
        return Err(frontier_error(
            counter,
            expected_length,
            ClaimFrontierInvalidReason::AggregateLedger,
        ));
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn validate_numeric_union_for_test(
    first_value: u128,
    expected_length: u128,
    ranges: &[(u128, u128)],
) -> Result<(), ClaimFrontierError> {
    let mut segments: Vec<_> = ranges
        .iter()
        .map(|(start, length)| NumericSegment {
            start: *start,
            length: *length,
            class: Some(SequenceClass::Exit),
            immutable: false,
        })
        .collect();
    validate_numeric_segments(
        ClaimFrontierCounter::DeliverySequence,
        first_value,
        expected_length,
        &mut segments,
        &[expected_length, 0, 0, 0, 0, 0, 0, 0],
    )
}

impl From<SequenceClass> for usize {
    fn from(value: SequenceClass) -> Self {
        value as Self
    }
}

impl From<OrderClass> for usize {
    fn from(value: OrderClass) -> Self {
        value as Self
    }
}

const fn sequence_candidate_class(candidate: ImmutableSequenceCandidate) -> SequenceClass {
    match candidate {
        ImmutableSequenceCandidate::BindingTerminal { .. } => SequenceClass::Terminal,
        ImmutableSequenceCandidate::Marker(marker) => match marker.current_owner {
            MarkerSequenceOwner::Marker => SequenceClass::Marker,
            MarkerSequenceOwner::ConditionalProduct(SequenceProductClass::LiveTimesTerminal) => {
                SequenceClass::LiveTimesTerminal
            }
            MarkerSequenceOwner::ConditionalProduct(
                SequenceProductClass::LiveTimesReplacementTerminal,
            ) => SequenceClass::LiveTimesReplacementTerminal,
            MarkerSequenceOwner::ConditionalProduct(SequenceProductClass::OtherLiveTimesExit) => {
                SequenceClass::OtherLiveTimesExit
            }
        },
    }
}

fn sequence_expected_counts(ledger: SequenceLedger) -> [u128; 8] {
    let budget = ledger.budget();
    [
        u128::from(budget.e),
        u128::from(budget.t),
        u128::from(budget.m),
        u128::from(budget.rs),
        u128::from(budget.rt),
        budget.l_times_t,
        budget.l_times_rt,
        budget.l_other_times_e,
    ]
}

fn order_expected_counts(ledger: OrderLedger) -> [u128; 4] {
    let claims = ledger.claims();
    [
        u128::from(claims.active_binding_terminals()),
        u128::from(claims.membership_exits()),
        u128::from(claims.recovery_operation()),
        u128::from(claims.recovery_replacement_terminal()),
    ]
}

fn validate_sequence_recovery(
    active: &ActiveIdentityRanks,
    recovery: Option<RecoverySequenceBlockRestore>,
    provenance: Option<RecoveryClaimProvenance>,
    ledger: SequenceLedger,
    frontier_length: u128,
) -> Result<(), ClaimFrontierError> {
    let expected = ledger.claims().recovery();
    match (expected, recovery, provenance) {
        (RecoverySequenceReserve::None, None, None) => Ok(()),
        (RecoverySequenceReserve::DetachedCredentialRecovery, None, _)
        | (RecoverySequenceReserve::DetachedCredentialRecovery, Some(_), None)
        | (RecoverySequenceReserve::None, None, Some(_)) => Err(sequence_error(
            frontier_length,
            ClaimFrontierInvalidReason::RecoveryBlock,
        )),
        (RecoverySequenceReserve::None, Some(block), _) => Err(sequence_error(
            sequence_ordinal(ledger, block_start_sequence(block)),
            ClaimFrontierInvalidReason::RecoveryBlock,
        )),
        (RecoverySequenceReserve::DetachedCredentialRecovery, Some(block), Some(provenance)) => {
            let block_ordinal = sequence_ordinal(ledger, block_start_sequence(block));
            let expected_recovery_attach = block
                .terminal
                .map_or(Some(block.recovery_attach_seq), |terminal| {
                    terminal.delivery_seq.checked_add(1)
                });
            if expected_recovery_attach != Some(block.recovery_attach_seq) {
                return Err(sequence_error(
                    block_ordinal + 1,
                    ClaimFrontierInvalidReason::RecoveryBlock,
                ));
            }
            if block.recovery_attach_seq.checked_add(1) != Some(block.replacement_terminal_seq) {
                return Err(sequence_error(
                    block_ordinal + u128::from(block.terminal.is_some()) + 1,
                    ClaimFrontierInvalidReason::RecoveryBlock,
                ));
            }
            let Some(participant) = active_participant(active, provenance.participant_index) else {
                return Err(sequence_error(
                    block_ordinal,
                    ClaimFrontierInvalidReason::LogicalOwner,
                ));
            };
            let expected_binding = match provenance.phase {
                RecoveryClaimPhase::PreFate => {
                    FrontierBinding::Bound(provenance.prior_binding_epoch)
                }
                RecoveryClaimPhase::PostFate => {
                    FrontierBinding::Detached(provenance.prior_binding_epoch)
                }
                RecoveryClaimPhase::RecoveredBound => {
                    FrontierBinding::Bound(provenance.current_binding_epoch)
                }
            };
            if participant.binding != expected_binding {
                return Err(sequence_error(
                    block_ordinal,
                    ClaimFrontierInvalidReason::LogicalOwner,
                ));
            }
            let terminal_valid = match (provenance.phase, block.terminal) {
                (RecoveryClaimPhase::PreFate, Some(terminal)) => {
                    terminal.owner.participant_index == provenance.participant_index
                        && terminal.owner.binding_epoch == provenance.prior_binding_epoch
                }
                (RecoveryClaimPhase::PostFate | RecoveryClaimPhase::RecoveredBound, None) => true,
                _ => false,
            };
            if !terminal_valid {
                return Err(sequence_error(
                    block_ordinal,
                    ClaimFrontierInvalidReason::RecoveryBlock,
                ));
            }
            Ok(())
        }
    }
}

fn validate_sequence_candidates(
    active: &ActiveIdentityRanks,
    candidates: &[ImmutableSequenceCandidate],
    retained_floor: u128,
    retained_records: &[RetainedCausalRecord],
    historical_causal_authorities: &[HistoricalCausalAuthority],
    ledger: SequenceLedger,
) -> Result<(), ClaimFrontierError> {
    let mut seen_keys = Vec::new();
    let mut previous_sequence = None;
    let mut previous_order = retained_records.last().map(|record| record.admission_order);
    for candidate in candidates {
        let ordinal = sequence_ordinal(ledger, candidate.delivery_seq());
        let order = candidate.admission_order();
        if previous_sequence.is_some_and(|previous| previous >= candidate.delivery_seq())
            || previous_order.is_some_and(|previous| previous >= order)
            || seen_keys.contains(&order)
        {
            return Err(sequence_error(
                ordinal,
                ClaimFrontierInvalidReason::CandidateKey,
            ));
        }
        previous_sequence = Some(candidate.delivery_seq());
        previous_order = Some(order);
        seen_keys.push(order);
        match candidate {
            ImmutableSequenceCandidate::BindingTerminal { owner, .. } => {
                if order.candidate_phase() != CandidatePhase::BindingTerminal
                    || order.participant_index() != owner.participant_index
                    || !terminal_matches_active(active, *owner)
                {
                    return Err(sequence_error(
                        ordinal,
                        ClaimFrontierInvalidReason::CandidateKey,
                    ));
                }
            }
            ImmutableSequenceCandidate::Marker(marker) => {
                let Some(participant) = active_participant(active, order.participant_index())
                else {
                    return Err(sequence_error(
                        ordinal,
                        ClaimFrontierInvalidReason::LogicalOwner,
                    ));
                };
                if order.candidate_phase() != CandidatePhase::CompactionMarker
                    || marker.current_owner != MarkerSequenceOwner::Marker
                    || marker.target_binding != participant.binding
                    || marker.abandoned_after != participant.cursor
                    || marker.abandoned_after > marker.abandoned_through
                    || u128::from(marker.physical_floor_at_decision) != retained_floor
                    || u128::from(marker.physical_floor_at_decision)
                        > u128::from(marker.abandoned_through) + 1
                    || marker.abandoned_through >= marker.delivery_seq
                    || !marker_provenance_targets(marker.provenance, order.participant_index())
                    || !marker_has_causal_authority(
                        *marker,
                        retained_records,
                        historical_causal_authorities,
                    )
                {
                    return Err(sequence_error(
                        ordinal,
                        ClaimFrontierInvalidReason::CandidateKey,
                    ));
                }
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TerminalOccurrenceKind {
    Movable,
    Candidate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TerminalOccurrence {
    owner: BindingTerminalOwner,
    ordinal: u128,
    kind: TerminalOccurrenceKind,
}

fn validate_sequence_direct_owners(
    active: &ActiveIdentityRanks,
    movable: &[MovableSequenceClaim],
    candidates: &[ImmutableSequenceCandidate],
    recovery: Option<RecoverySequenceBlockRestore>,
    ledger: SequenceLedger,
) -> Result<Vec<BindingTerminalOwner>, ClaimFrontierError> {
    let (mut exit_owners, terminal_occurrences) =
        collect_sequence_direct_owners(active, movable, candidates, recovery, ledger)?;
    validate_sequence_exit_owners(active, &mut exit_owners, ledger)?;
    validate_sequence_terminal_owners(active, &terminal_occurrences, ledger)?;
    let mut owners: Vec<_> = terminal_occurrences
        .into_iter()
        .map(|occurrence| occurrence.owner)
        .collect();
    owners.sort_by_key(|owner| (owner.participant_index, owner.binding_epoch));
    Ok(owners)
}

fn collect_sequence_direct_owners(
    active: &ActiveIdentityRanks,
    movable: &[MovableSequenceClaim],
    candidates: &[ImmutableSequenceCandidate],
    recovery: Option<RecoverySequenceBlockRestore>,
    ledger: SequenceLedger,
) -> Result<(Vec<ParticipantId>, Vec<TerminalOccurrence>), ClaimFrontierError> {
    let mut ordered_movable = movable.to_vec();
    ordered_movable.sort_by_key(|claim| claim.delivery_seq);
    let mut exit_owners = Vec::new();
    let mut terminal_occurrences = Vec::new();
    for claim in ordered_movable {
        let ordinal = sequence_ordinal(ledger, claim.delivery_seq);
        match claim.owner {
            SequenceDirectOwner::MembershipExit { participant_index } => {
                if !active.contains(participant_index) || exit_owners.contains(&participant_index) {
                    return Err(sequence_error(
                        ordinal,
                        ClaimFrontierInvalidReason::LogicalOwner,
                    ));
                }
                exit_owners.push(participant_index);
            }
            SequenceDirectOwner::BindingTerminal(owner) => {
                if !terminal_matches_bound(active, owner) {
                    return Err(sequence_error(
                        ordinal,
                        ClaimFrontierInvalidReason::LogicalOwner,
                    ));
                }
                push_terminal_occurrence(
                    &mut terminal_occurrences,
                    owner,
                    ordinal,
                    TerminalOccurrenceKind::Movable,
                )?;
            }
        }
    }
    for candidate in candidates {
        if let ImmutableSequenceCandidate::BindingTerminal { owner, .. } = candidate {
            let ordinal = sequence_ordinal(ledger, candidate.delivery_seq());
            push_terminal_occurrence(
                &mut terminal_occurrences,
                *owner,
                ordinal,
                TerminalOccurrenceKind::Candidate,
            )?;
        }
    }
    if let Some(terminal) = recovery.and_then(|block| block.terminal) {
        let ordinal = sequence_ordinal(ledger, terminal.delivery_seq);
        push_terminal_occurrence(
            &mut terminal_occurrences,
            terminal.owner,
            ordinal,
            TerminalOccurrenceKind::Movable,
        )?;
    }
    Ok((exit_owners, terminal_occurrences))
}

fn push_terminal_occurrence(
    occurrences: &mut Vec<TerminalOccurrence>,
    owner: BindingTerminalOwner,
    ordinal: u128,
    kind: TerminalOccurrenceKind,
) -> Result<(), ClaimFrontierError> {
    if occurrences
        .iter()
        .any(|occurrence| occurrence.owner == owner)
    {
        return Err(sequence_error(
            ordinal,
            ClaimFrontierInvalidReason::LogicalOwner,
        ));
    }
    occurrences.push(TerminalOccurrence {
        owner,
        ordinal,
        kind,
    });
    Ok(())
}

fn validate_sequence_exit_owners(
    active: &ActiveIdentityRanks,
    exit_owners: &mut [ParticipantId],
    ledger: SequenceLedger,
) -> Result<(), ClaimFrontierError> {
    exit_owners.sort_unstable();
    if exit_owners.len() != active.participants.len()
        || !exit_owners.iter().copied().eq(active
            .participants
            .iter()
            .map(|participant| participant.participant_index))
    {
        return Err(sequence_error(
            ledger.required_reserve(),
            ClaimFrontierInvalidReason::LogicalOwner,
        ));
    }
    Ok(())
}

fn validate_sequence_terminal_owners(
    active: &ActiveIdentityRanks,
    terminal_occurrences: &[TerminalOccurrence],
    ledger: SequenceLedger,
) -> Result<(), ClaimFrontierError> {
    for participant in &active.participants {
        let matching: Vec<_> = terminal_occurrences
            .iter()
            .filter(|occurrence| {
                occurrence.owner.participant_index == participant.participant_index
            })
            .copied()
            .collect();
        match participant.binding {
            FrontierBinding::Bound(epoch) => {
                if !matches!(matching.as_slice(), [occurrence] if occurrence.owner.binding_epoch == epoch)
                {
                    return Err(sequence_error(
                        matching.first().map_or_else(
                            || ledger.required_reserve(),
                            |occurrence| occurrence.ordinal,
                        ),
                        ClaimFrontierInvalidReason::LogicalOwner,
                    ));
                }
            }
            FrontierBinding::Detached(epoch) => {
                if matching.len() > 1
                    || matching.first().is_some_and(|occurrence| {
                        occurrence.owner.binding_epoch != epoch
                            || occurrence.kind != TerminalOccurrenceKind::Candidate
                    })
                {
                    return Err(sequence_error(
                        matching.first().map_or_else(
                            || ledger.required_reserve(),
                            |occurrence| occurrence.ordinal,
                        ),
                        ClaimFrontierInvalidReason::LogicalOwner,
                    ));
                }
            }
        }
    }
    Ok(())
}

fn validate_sequence_products(
    active: &ActiveIdentityRanks,
    restore: SequenceProductRangesRestore,
    terminal_owners: &[BindingTerminalOwner],
    recovery: Option<RecoverySequenceBlockRestore>,
    recovery_provenance: Option<RecoveryClaimProvenance>,
    ledger: SequenceLedger,
) -> Result<SequenceProductRanges, ClaimFrontierError> {
    let live_count = usize_to_u64(active.participants.len());
    let other_count = live_count.saturating_sub(1);
    let live_times_terminal = validate_terminal_product_ranges(
        restore.live_times_terminal,
        terminal_owners,
        live_count,
        ledger,
    )?;
    let live_times_replacement_terminal = validate_replacement_product_range(
        restore.live_times_replacement_terminal,
        recovery,
        recovery_provenance,
        live_count,
        ledger,
    )?;
    let other_live_times_exit =
        validate_exit_product_ranges(active, restore.other_live_times_exit, other_count, ledger)?;
    Ok(SequenceProductRanges {
        live_times_terminal,
        live_times_replacement_terminal,
        other_live_times_exit,
    })
}

fn validate_terminal_product_ranges(
    mut ranges: Vec<TerminalProductRangeRestore>,
    terminal_owners: &[BindingTerminalOwner],
    live_count: u64,
    ledger: SequenceLedger,
) -> Result<Vec<TerminalProductRange>, ClaimFrontierError> {
    ranges.sort_by_key(|range| range.start);
    let mut seen_terminals = Vec::new();
    let mut live_times_terminal = Vec::new();
    for range in ranges {
        let ordinal = sequence_ordinal(ledger, range.start);
        if range.length != live_count
            || !terminal_owners.contains(&range.terminal)
            || seen_terminals.contains(&range.terminal)
        {
            return Err(sequence_error(
                ordinal,
                ClaimFrontierInvalidReason::ProductRange,
            ));
        }
        seen_terminals.push(range.terminal);
        live_times_terminal.push(TerminalProductRange {
            start: range.start,
            length: range.length,
            terminal: range.terminal,
        });
    }
    if seen_terminals.len() != terminal_owners.len() {
        return Err(sequence_error(
            ledger.required_reserve(),
            ClaimFrontierInvalidReason::ProductRange,
        ));
    }
    Ok(live_times_terminal)
}

fn validate_replacement_product_range(
    range: Option<ReplacementTerminalProductRangeRestore>,
    recovery: Option<RecoverySequenceBlockRestore>,
    recovery_provenance: Option<RecoveryClaimProvenance>,
    live_count: u64,
    ledger: SequenceLedger,
) -> Result<Option<ReplacementTerminalProductRange>, ClaimFrontierError> {
    let validated = match (range, recovery, recovery_provenance) {
        (None, None, None) => None,
        (Some(range), Some(_), Some(provenance)) if range.length == live_count => {
            Some(ReplacementTerminalProductRange {
                start: range.start,
                length: range.length,
                participant_index: provenance.participant_index,
                marker_delivery_seq: provenance.marker_delivery_seq,
                prior_binding_epoch: provenance.prior_binding_epoch,
            })
        }
        (Some(range), _, _) => {
            return Err(sequence_error(
                sequence_ordinal(ledger, range.start),
                ClaimFrontierInvalidReason::ProductRange,
            ));
        }
        (None, Some(_), _) | (None, None, Some(_)) => {
            return Err(sequence_error(
                ledger.required_reserve(),
                ClaimFrontierInvalidReason::ProductRange,
            ));
        }
    };
    Ok(validated)
}

fn validate_exit_product_ranges(
    active: &ActiveIdentityRanks,
    mut ranges: Vec<ExitProductRangeRestore>,
    other_count: u64,
    ledger: SequenceLedger,
) -> Result<Vec<ExitProductRange>, ClaimFrontierError> {
    ranges.sort_by_key(|range| range.start);
    let mut seen_exits = Vec::new();
    let mut other_live_times_exit = Vec::new();
    if other_count == 0 && !ranges.is_empty() {
        return Err(sequence_error(
            ledger.required_reserve(),
            ClaimFrontierInvalidReason::ProductRange,
        ));
    }
    for range in ranges {
        let ordinal = sequence_ordinal(ledger, range.start);
        if range.length != other_count
            || !active.contains(range.exit_participant)
            || seen_exits.contains(&range.exit_participant)
        {
            return Err(sequence_error(
                ordinal,
                ClaimFrontierInvalidReason::ProductRange,
            ));
        }
        seen_exits.push(range.exit_participant);
        other_live_times_exit.push(ExitProductRange {
            start: range.start,
            length: range.length,
            exit_participant: range.exit_participant,
        });
    }
    seen_exits.sort_unstable();
    let expected_exit_ranges = if other_count == 0 {
        0
    } else {
        active.participants.len()
    };
    if seen_exits.len() != expected_exit_ranges
        || !seen_exits.iter().copied().eq(active
            .participants
            .iter()
            .take(expected_exit_ranges)
            .map(|participant| participant.participant_index))
    {
        return Err(sequence_error(
            ledger.required_reserve(),
            ClaimFrontierInvalidReason::ProductRange,
        ));
    }
    Ok(other_live_times_exit)
}

fn validate_order_recovery(
    active: &ActiveIdentityRanks,
    recovery: Option<RecoveryOrderBlockRestore>,
    provenance: Option<RecoveryClaimProvenance>,
    ledger: OrderLedger,
    frontier_length: u128,
) -> Result<(), ClaimFrontierError> {
    let claims = ledger.claims();
    let expected = claims.recovery_operation() && claims.recovery_replacement_terminal();
    match (expected, recovery, provenance) {
        (false, None, None) => Ok(()),
        (true, None, _) | (true, Some(_), None) | (false, None, Some(_)) => Err(order_error(
            frontier_length,
            ClaimFrontierInvalidReason::RecoveryBlock,
        )),
        (false, Some(block), _) => Err(order_error(
            order_ordinal(ledger, block_start_order(block)),
            ClaimFrontierInvalidReason::RecoveryBlock,
        )),
        (true, Some(block), Some(provenance)) => {
            let block_ordinal = order_ordinal(ledger, block_start_order(block));
            let expected_recovery_operation = block
                .active_binding
                .map_or(Some(block.recovery_operation_order), |active_binding| {
                    active_binding.transaction_order.checked_add(1)
                });
            if expected_recovery_operation != Some(block.recovery_operation_order) {
                return Err(order_error(
                    block_ordinal + 1,
                    ClaimFrontierInvalidReason::RecoveryBlock,
                ));
            }
            if block.recovery_operation_order.checked_add(1)
                != Some(block.replacement_terminal_order)
            {
                return Err(order_error(
                    block_ordinal + u128::from(block.active_binding.is_some()) + 1,
                    ClaimFrontierInvalidReason::RecoveryBlock,
                ));
            }
            let Some(participant) = active_participant(active, provenance.participant_index) else {
                return Err(order_error(
                    block_ordinal,
                    ClaimFrontierInvalidReason::LogicalOwner,
                ));
            };
            let expected_binding = match provenance.phase {
                RecoveryClaimPhase::PreFate => {
                    FrontierBinding::Bound(provenance.prior_binding_epoch)
                }
                RecoveryClaimPhase::PostFate => {
                    FrontierBinding::Detached(provenance.prior_binding_epoch)
                }
                RecoveryClaimPhase::RecoveredBound => {
                    FrontierBinding::Bound(provenance.current_binding_epoch)
                }
            };
            if participant.binding != expected_binding {
                return Err(order_error(
                    block_ordinal,
                    ClaimFrontierInvalidReason::LogicalOwner,
                ));
            }
            let active_binding_valid = match (provenance.phase, block.active_binding) {
                (RecoveryClaimPhase::PreFate, Some(active_binding)) => {
                    active_binding.owner.participant_index == provenance.participant_index
                        && active_binding.owner.binding_epoch == provenance.prior_binding_epoch
                }
                (RecoveryClaimPhase::PostFate | RecoveryClaimPhase::RecoveredBound, None) => true,
                _ => false,
            };
            if !active_binding_valid {
                return Err(order_error(
                    block_ordinal,
                    ClaimFrontierInvalidReason::RecoveryBlock,
                ));
            }
            Ok(())
        }
    }
}

fn validate_order_candidates(
    restore: &[ImmutableOrderCandidateMajorRestore],
    ledger: OrderLedger,
) -> Result<Vec<ImmutableOrderCandidateMajor>, ClaimFrontierError> {
    let mut groups = restore.to_vec();
    groups.sort_by_key(|group| group.transaction_order);
    let mut seen_keys = Vec::new();
    let mut previous_major = None;
    let mut validated = Vec::new();
    for group in groups {
        let ordinal = order_ordinal(ledger, group.transaction_order);
        let below_allocated_high = matches!(
            ledger.high(),
            OrderHigh::Allocated(high) if group.transaction_order < high
        );
        if group.candidate_keys.is_empty()
            || previous_major == Some(group.transaction_order)
            || below_allocated_high
        {
            return Err(order_error(
                ordinal,
                ClaimFrontierInvalidReason::CandidateKey,
            ));
        }
        previous_major = Some(group.transaction_order);
        let mut previous = None;
        for key in &group.candidate_keys {
            if key.transaction_order() != group.transaction_order
                || previous.is_some_and(|previous| previous >= *key)
                || seen_keys.contains(key)
            {
                return Err(order_error(
                    ordinal,
                    ClaimFrontierInvalidReason::CandidateKey,
                ));
            }
            previous = Some(*key);
            seen_keys.push(*key);
        }
        validated.push(ImmutableOrderCandidateMajor {
            transaction_order: group.transaction_order,
            candidate_keys: group.candidate_keys,
        });
    }
    Ok(validated)
}

fn validate_order_direct_owners(
    active: &ActiveIdentityRanks,
    movable: &[MovableOrderClaim],
    recovery: Option<RecoveryOrderBlockRestore>,
    ledger: OrderLedger,
) -> Result<(), ClaimFrontierError> {
    let mut ordered = movable.to_vec();
    ordered.sort_by_key(|claim| claim.transaction_order);
    let mut exits = Vec::new();
    let mut terminals = Vec::new();
    for claim in ordered {
        let ordinal = order_ordinal(ledger, claim.transaction_order);
        match claim.owner {
            OrderDirectOwner::MembershipExit { participant_index } => {
                if !active.contains(participant_index) || exits.contains(&participant_index) {
                    return Err(order_error(
                        ordinal,
                        ClaimFrontierInvalidReason::LogicalOwner,
                    ));
                }
                exits.push(participant_index);
            }
            OrderDirectOwner::ActiveBindingTerminal(owner) => {
                if !terminal_matches_bound(active, owner) || terminals.contains(&owner) {
                    return Err(order_error(
                        ordinal,
                        ClaimFrontierInvalidReason::LogicalOwner,
                    ));
                }
                terminals.push(owner);
            }
        }
    }
    if let Some(active_binding) = recovery.and_then(|block| block.active_binding) {
        let ordinal = order_ordinal(ledger, active_binding.transaction_order);
        if !terminal_matches_bound(active, active_binding.owner)
            || terminals.contains(&active_binding.owner)
        {
            return Err(order_error(
                ordinal,
                ClaimFrontierInvalidReason::LogicalOwner,
            ));
        }
        terminals.push(active_binding.owner);
    }
    exits.sort_unstable();
    if exits.len() != active.participants.len()
        || !exits.iter().copied().eq(active
            .participants
            .iter()
            .map(|participant| participant.participant_index))
    {
        return Err(order_error(
            ledger.claims().total(),
            ClaimFrontierInvalidReason::LogicalOwner,
        ));
    }
    terminals.sort_by_key(|owner| (owner.participant_index, owner.binding_epoch));
    if usize_to_u128(terminals.len()) != u128::from(ledger.claims().active_binding_terminals()) {
        return Err(order_error(
            ledger.claims().total(),
            ClaimFrontierInvalidReason::LogicalOwner,
        ));
    }
    Ok(())
}

fn validate_cross_counter(
    sequence: &SequenceClaimFrontier,
    order: &OrderClaimFrontier,
) -> Result<(), ClaimFrontierError> {
    match (sequence.recovery, order.recovery) {
        (None, None) => {}
        (Some(sequence_block), Some(order_block))
            if sequence_block.participant_index == order_block.participant_index
                && sequence_block.marker_delivery_seq == order_block.marker_delivery_seq
                && sequence_block.recovered_binding_epoch
                    == order_block.recovered_binding_epoch
                && sequence_block.terminal.map(|terminal| terminal.owner)
                    == order_block.active_binding.map(|active| active.owner) => {}
        (Some(sequence_block), _) => {
            return Err(sequence_error(
                sequence_ordinal(
                    sequence.ledger,
                    block_start_validated_sequence(sequence_block),
                ),
                ClaimFrontierInvalidReason::RecoveryBlock,
            ));
        }
        (None, Some(order_block)) => {
            return Err(order_error(
                order_ordinal(order.ledger, block_start_validated_order(order_block)),
                ClaimFrontierInvalidReason::RecoveryBlock,
            ));
        }
    }

    let mut order_candidate_keys = Vec::new();
    for group in &order.immutable_candidates {
        order_candidate_keys.extend(group.candidate_keys.iter().copied());
    }
    for candidate in &sequence.immutable_candidates {
        let key = candidate.admission_order();
        if !order_candidate_keys.contains(&key) {
            return Err(sequence_error(
                sequence_ordinal(sequence.ledger, candidate.delivery_seq()),
                ClaimFrontierInvalidReason::CandidateKey,
            ));
        }
    }
    for group in &order.immutable_candidates {
        for key in &group.candidate_keys {
            if !sequence
                .immutable_candidates
                .iter()
                .any(|candidate| candidate.admission_order() == *key)
            {
                return Err(order_error(
                    order_ordinal(order.ledger, group.transaction_order),
                    ClaimFrontierInvalidReason::CandidateKey,
                ));
            }
        }
    }

    let mut sequence_movable_terminals = Vec::new();
    for claim in &sequence.movable_claims {
        if let SequenceDirectOwner::BindingTerminal(owner) = claim.owner {
            sequence_movable_terminals.push(owner);
        }
    }
    if let Some(terminal) = sequence.recovery.and_then(|block| block.terminal) {
        sequence_movable_terminals.push(terminal.owner);
    }
    let mut order_movable_terminals = Vec::new();
    for claim in &order.movable_claims {
        if let OrderDirectOwner::ActiveBindingTerminal(owner) = claim.owner {
            order_movable_terminals.push(owner);
        }
    }
    if let Some(active_binding) = order.recovery.and_then(|block| block.active_binding) {
        order_movable_terminals.push(active_binding.owner);
    }
    sequence_movable_terminals.sort_by_key(|owner| (owner.participant_index, owner.binding_epoch));
    order_movable_terminals.sort_by_key(|owner| (owner.participant_index, owner.binding_epoch));
    if sequence_movable_terminals != order_movable_terminals {
        return Err(sequence_error(
            sequence.ledger.required_reserve(),
            ClaimFrontierInvalidReason::LogicalOwner,
        ));
    }
    Ok(())
}

const fn marker_provenance_targets(provenance: MarkerProvenance, target: ParticipantId) -> bool {
    match provenance {
        MarkerProvenance::NonProductM => true,
        MarkerProvenance::TerminalProduct {
            affected_participant,
            ..
        } => affected_participant == target,
        MarkerProvenance::ExitProduct {
            exit_participant,
            remaining_participant,
        } => exit_participant != remaining_participant && remaining_participant == target,
    }
}

fn marker_has_causal_authority(
    marker: MarkerCandidateAuthority,
    records: &[RetainedCausalRecord],
    historical: &[HistoricalCausalAuthority],
) -> bool {
    if marker.provenance == MarkerProvenance::NonProductM {
        return true;
    }
    let retained_match = records.iter().any(|record| {
        if record.admission_order.transaction_order() != marker.admission_order.transaction_order()
        {
            return false;
        }
        match marker.provenance {
            MarkerProvenance::NonProductM => true,
            MarkerProvenance::TerminalProduct {
                terminal: TerminalProductSource::Binding(owner),
                ..
            } => matches!(
                record.kind,
                RetainedCausalRecordKind::BindingTerminal(actual) if actual == owner
            ),
            MarkerProvenance::TerminalProduct {
                terminal:
                    TerminalProductSource::RecoveryReplacement {
                        participant_index,
                        binding_epoch,
                    },
                ..
            } => matches!(
                record.kind,
                RetainedCausalRecordKind::BindingTerminal(owner)
                    if owner.participant_index == participant_index
                        && owner.binding_epoch == binding_epoch
            ),
            MarkerProvenance::ExitProduct {
                exit_participant, ..
            } => matches!(
                record.kind,
                RetainedCausalRecordKind::MembershipExit { participant_index }
                    if participant_index == exit_participant
            ),
        }
    });
    retained_match
        || historical
            .iter()
            .any(|authority| match (marker.provenance, authority.kind) {
                (
                    MarkerProvenance::TerminalProduct {
                        terminal: TerminalProductSource::Binding(expected),
                        ..
                    },
                    HistoricalCausalKind::BindingTerminal(owner),
                ) => {
                    owner == expected
                        && authority.admission_order.transaction_order()
                            == marker.admission_order.transaction_order()
                }
                (
                    MarkerProvenance::TerminalProduct {
                        terminal:
                            TerminalProductSource::RecoveryReplacement {
                                participant_index,
                                binding_epoch,
                            },
                        ..
                    },
                    HistoricalCausalKind::BindingTerminal(owner),
                ) => {
                    owner.participant_index == participant_index
                        && owner.binding_epoch == binding_epoch
                        && authority.admission_order.transaction_order()
                            == marker.admission_order.transaction_order()
                }
                (
                    MarkerProvenance::ExitProduct {
                        exit_participant, ..
                    },
                    HistoricalCausalKind::MembershipExit(participant_index),
                ) => {
                    participant_index == exit_participant
                        && authority.admission_order.transaction_order()
                            == marker.admission_order.transaction_order()
                }
                _ => false,
            })
}

fn terminal_matches_active(active: &ActiveIdentityRanks, owner: BindingTerminalOwner) -> bool {
    active_participant(active, owner.participant_index)
        .is_some_and(|participant| binding_epoch(participant.binding) == owner.binding_epoch)
}

fn terminal_matches_bound(active: &ActiveIdentityRanks, owner: BindingTerminalOwner) -> bool {
    active_participant(active, owner.participant_index).is_some_and(|participant| {
        participant.binding == FrontierBinding::Bound(owner.binding_epoch)
    })
}

fn active_participant(
    active: &ActiveIdentityRanks,
    participant_index: ParticipantId,
) -> Option<FrontierParticipant> {
    active
        .participants
        .binary_search_by_key(&participant_index, |participant| {
            participant.participant_index
        })
        .ok()
        .and_then(|index| active.participants.get(index))
        .copied()
}

const fn binding_epoch(binding: FrontierBinding) -> BindingEpoch {
    match binding {
        FrontierBinding::Bound(epoch) | FrontierBinding::Detached(epoch) => epoch,
    }
}

fn block_start_sequence(block: RecoverySequenceBlockRestore) -> DeliverySeq {
    block
        .terminal
        .map_or(block.recovery_attach_seq, |terminal| terminal.delivery_seq)
}

fn block_start_validated_sequence(block: RecoverySequenceBlock) -> DeliverySeq {
    block
        .terminal
        .map_or(block.recovery_attach_seq, |terminal| terminal.delivery_seq)
}

fn block_start_order(block: RecoveryOrderBlockRestore) -> TransactionOrder {
    block
        .active_binding
        .map_or(block.recovery_operation_order, |active_binding| {
            active_binding.transaction_order
        })
}

fn block_start_validated_order(block: RecoveryOrderBlock) -> TransactionOrder {
    block
        .active_binding
        .map_or(block.recovery_operation_order, |active_binding| {
            active_binding.transaction_order
        })
}

fn order_frontier_start(high: OrderHigh) -> u128 {
    match high {
        OrderHigh::Empty => 0,
        OrderHigh::Allocated(high) => u128::from(high) + 1,
    }
}

const fn order_is_above_high(value: TransactionOrder, high: OrderHigh) -> bool {
    match high {
        OrderHigh::Empty => true,
        OrderHigh::Allocated(high) => value > high,
    }
}

fn order_frontier_candidate_count(restore: &OrderClaimFrontierRestore, high: OrderHigh) -> u128 {
    usize_to_u128(
        restore
            .immutable_candidates
            .iter()
            .filter(|candidate| order_is_above_high(candidate.transaction_order, high))
            .count(),
    )
}

fn sequence_ordinal(ledger: SequenceLedger, value: DeliverySeq) -> u128 {
    u128::from(value).saturating_sub(u128::from(ledger.high_watermark()) + 1)
}

fn order_ordinal(ledger: OrderLedger, value: TransactionOrder) -> u128 {
    u128::from(value).saturating_sub(order_frontier_start(ledger.high()))
}

fn checked_rank_value(start: DeliverySeq, active_rank: usize) -> Option<DeliverySeq> {
    let rank = u64::try_from(active_rank).ok()?;
    start.checked_add(rank)
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).map_or(u64::MAX, core::convert::identity)
}

fn usize_to_u128(value: usize) -> u128 {
    u64::try_from(value).map_or(u128::MAX, u128::from)
}

fn rank_index(rank: usize) -> u128 {
    usize_to_u128(rank)
}

const fn frontier_error(
    counter: ClaimFrontierCounter,
    first_bad_position: u128,
    reason: ClaimFrontierInvalidReason,
) -> ClaimFrontierError {
    ClaimFrontierError {
        counter,
        first_bad_position,
        reason,
    }
}

const fn sequence_error(
    first_bad_position: u128,
    reason: ClaimFrontierInvalidReason,
) -> ClaimFrontierError {
    frontier_error(
        ClaimFrontierCounter::DeliverySequence,
        first_bad_position,
        reason,
    )
}

const fn order_error(
    first_bad_position: u128,
    reason: ClaimFrontierInvalidReason,
) -> ClaimFrontierError {
    frontier_error(
        ClaimFrontierCounter::TransactionOrder,
        first_bad_position,
        reason,
    )
}
