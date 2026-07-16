//! Sealed ordinary-record retention and marker fixed-point projection.
//!
//! This module implements the factual, zero-debt portion of frozen
//! `PARTICIPANT-CONTRACT.md` R-C4.  It deliberately accepts complete keyed
//! retained-row charges rather than a caller-supplied retained total or marker
//! count.  The final frontier transition consumes this projection so storage
//! cannot invent a floor, marker set, resulting ledger, or baseline.

use alloc::{boxed::Box, vec::Vec};

use crate::{
    algebra::{
        FloorComputation, ResourceDimension, ResourceVector, WideResourceVector, floor_transition,
        zero_debt_admission,
    },
    outcome::CandidatePhase,
    wire::{
        BindingEpoch, DeliverySeq, OrderAllocatingEnvelope, ParticipantId, RecordAdmissionEnvelope,
        SequenceAllocatingEnvelope,
    },
};

use super::super::{
    AdmissionOrder, ClaimFrontiers, ClosureAccounting, ClosureAccountingError, ClosureState,
    FrontierBinding, FrontierParticipant, ImmutableSequenceCandidate, MarkerCandidateAuthority,
    MarkerProvenance, MarkerSequenceOwner, ObserverFloorPermit, OrderAdmissionError,
    OrderAllocation, OrderLedger, RemainingClosurePermit, RequiredCapacityPlan,
    RequiredCapacityPlanError, RetainedCausalRecord, RetainedCausalRecordKind, SequenceAdmission,
    SequenceAdmissionError, SequenceLedger, admit_sequence, allocate_order,
};

/// Exact durable charge keyed to one validated retained causal row.
///
/// The charge remains separate from [`RetainedCausalRecord`] because payload
/// bytes and storage framing belong to the server's durability schema.  The
/// projection requires a one-for-one key match before using any charge.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetainedRecordCharge {
    delivery_seq: DeliverySeq,
    admission_order: AdmissionOrder,
    encoded_charge: ResourceVector,
}

impl RetainedRecordCharge {
    /// Creates one factual keyed durable-row charge.
    #[must_use]
    pub const fn new(
        delivery_seq: DeliverySeq,
        admission_order: AdmissionOrder,
        encoded_charge: ResourceVector,
    ) -> Self {
        Self {
            delivery_seq,
            admission_order,
            encoded_charge,
        }
    }

    /// Returns the durable delivery key.
    #[must_use]
    pub const fn delivery_seq(self) -> DeliverySeq {
        self.delivery_seq
    }

    /// Returns the immutable causal key.
    #[must_use]
    pub const fn admission_order(self) -> AdmissionOrder {
        self.admission_order
    }

    /// Returns the exact entry/byte durability charge.
    #[must_use]
    pub const fn encoded_charge(self) -> ResourceVector {
        self.encoded_charge
    }
}

/// Signed closure limits used by one ordinary fixed-point projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrdinaryProjectionLimits {
    marker_max: ResourceVector,
    mandatory_bound: ResourceVector,
    full_recovery_claim: ResourceVector,
}

impl OrdinaryProjectionLimits {
    /// Captures the configured identity, marker, mandatory, and recovery bounds.
    #[must_use]
    pub const fn new(
        marker_max: ResourceVector,
        mandatory_bound: ResourceVector,
        full_recovery_claim: ResourceVector,
    ) -> Self {
        Self {
            marker_max,
            mandatory_bound,
            full_recovery_claim,
        }
    }

    /// Returns the generated maximum marker charge.
    #[must_use]
    pub const fn marker_max(self) -> ResourceVector {
        self.marker_max
    }

    /// Returns generated mandatory transaction envelope `Q`.
    #[must_use]
    pub const fn mandatory_bound(self) -> ResourceVector {
        self.mandatory_bound
    }

    /// Returns full transferable recovery occupancy `K`.
    #[must_use]
    pub const fn full_recovery_claim(self) -> ResourceVector {
        self.full_recovery_claim
    }
}

/// External facts and signed limits for one consuming ordinary projection.
///
/// Retained rows, marker-credit ownership, participant cursors, current floor,
/// immutable candidates, and both aggregate ledgers are deliberately absent:
/// [`ClaimFrontiers::project_ordinary_record`] supplies them from the one
/// validated frontier value it consumes. Keyed charges remain storage facts,
/// but cannot execute independently of those owned rows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrdinaryRecordProjectionInput {
    request: RecordAdmissionEnvelope,
    receiving_binding_epoch: BindingEpoch,
    encoded_record_charge: ResourceVector,
    retained_charges: Vec<RetainedRecordCharge>,
    observer_progress: DeliverySeq,
    closure_accounting: ClosureAccounting,
    limits: OrdinaryProjectionLimits,
}

impl OrdinaryRecordProjectionInput {
    /// Captures transport-derived record facts, exact keyed storage charges,
    /// current observer/accounting state, and signed capacity limits.
    #[must_use]
    pub const fn new(
        request: RecordAdmissionEnvelope,
        receiving_binding_epoch: BindingEpoch,
        encoded_record_charge: ResourceVector,
        retained_charges: Vec<RetainedRecordCharge>,
        observer_progress: DeliverySeq,
        closure_accounting: ClosureAccounting,
        limits: OrdinaryProjectionLimits,
    ) -> Self {
        Self {
            request,
            receiving_binding_epoch,
            encoded_record_charge,
            retained_charges,
            observer_progress,
            closure_accounting,
            limits,
        }
    }

    #[allow(clippy::type_complexity)]
    pub(in crate::lifecycle) fn as_parts(
        &self,
    ) -> (
        &RecordAdmissionEnvelope,
        BindingEpoch,
        ResourceVector,
        &[RetainedRecordCharge],
        DeliverySeq,
        ClosureAccounting,
        OrdinaryProjectionLimits,
    ) {
        (
            &self.request,
            self.receiving_binding_epoch,
            self.encoded_record_charge,
            &self.retained_charges,
            self.observer_progress,
            self.closure_accounting,
            self.limits,
        )
    }

    #[allow(clippy::type_complexity)]
    pub(in crate::lifecycle) fn into_parts(
        self,
    ) -> (
        RecordAdmissionEnvelope,
        BindingEpoch,
        ResourceVector,
        Vec<RetainedRecordCharge>,
        DeliverySeq,
        ClosureAccounting,
        OrdinaryProjectionLimits,
    ) {
        (
            self.request,
            self.receiving_binding_epoch,
            self.encoded_record_charge,
            self.retained_charges,
            self.observer_progress,
            self.closure_accounting,
            self.limits,
        )
    }
}

/// Sealed globally earlier candidate paired with its unchanged frontier state.
#[derive(Debug, PartialEq, Eq)]
pub struct OrdinaryRecordDrainFirst {
    pub(in crate::lifecycle) frontiers: ClaimFrontiers,
    pub(in crate::lifecycle) input: OrdinaryRecordProjectionInput,
    pub(in crate::lifecycle) candidate: ImmutableSequenceCandidate,
}

impl OrdinaryRecordDrainFirst {
    /// Returns the exact lowest candidate that prevented optional admission.
    #[must_use]
    pub const fn candidate(&self) -> ImmutableSequenceCandidate {
        self.candidate
    }

    /// Borrows the unchanged validated frontier state owned by this decision.
    #[must_use]
    pub const fn frontiers(&self) -> &ClaimFrontiers {
        &self.frontiers
    }

    /// Borrows the unchanged exact storage facts and signed limits.
    #[must_use]
    pub const fn projection_input(&self) -> &OrdinaryRecordProjectionInput {
        &self.input
    }

    /// Recovers the complete unchanged projection prestate.
    #[must_use]
    pub fn into_unchanged_parts(self) -> (ClaimFrontiers, OrdinaryRecordProjectionInput) {
        (self.frontiers, self.input)
    }
}

/// Recoverable noncommit from the consuming ordinary projection.
#[derive(Debug, PartialEq, Eq)]
pub struct OrdinaryRecordProjectionFailure {
    pub(in crate::lifecycle) frontiers: ClaimFrontiers,
    pub(in crate::lifecycle) input: OrdinaryRecordProjectionInput,
    pub(in crate::lifecycle) error: OrdinaryProjectionError,
}

impl OrdinaryRecordProjectionFailure {
    /// Borrows the exact projection error.
    #[must_use]
    pub const fn error(&self) -> &OrdinaryProjectionError {
        &self.error
    }

    /// Borrows the unchanged validated frontier aggregate.
    #[must_use]
    pub const fn frontiers(&self) -> &ClaimFrontiers {
        &self.frontiers
    }

    /// Borrows the unchanged exact storage facts and signed limits.
    #[must_use]
    pub const fn projection_input(&self) -> &OrdinaryRecordProjectionInput {
        &self.input
    }

    /// Recovers the unchanged projection prestate and selected error.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        ClaimFrontiers,
        OrdinaryRecordProjectionInput,
        OrdinaryProjectionError,
    ) {
        (self.frontiers, self.input, self.error)
    }
}

/// Sealed ordinary fixed-point poststate for one atomic durable transaction.
#[derive(Debug, PartialEq, Eq)]
pub struct ProjectedOrdinaryRecord {
    pub(in crate::lifecycle) frontiers: ClaimFrontiers,
    pub(in crate::lifecycle) floor: FloorComputation,
    pub(in crate::lifecycle) retained_charge: WideResourceVector,
    pub(in crate::lifecycle) baseline: WideResourceVector,
    pub(in crate::lifecycle) accounting: ClosureAccounting,
    pub(in crate::lifecycle) required_capacity: RequiredCapacityPlan,
    pub(in crate::lifecycle) order: OrderAllocation,
    pub(in crate::lifecycle) sequence: SequenceAdmission,
    pub(in crate::lifecycle) observer_floor: ObserverFloorPermit,
    pub(in crate::lifecycle) closure: RemainingClosurePermit,
    pub(in crate::lifecycle) caller_record: RetainedCausalRecord,
    pub(in crate::lifecycle) caller_charge: RetainedRecordCharge,
    pub(in crate::lifecycle) retained_charges: Vec<RetainedRecordCharge>,
    pub(in crate::lifecycle) new_marker_candidates: Vec<MarkerCandidateAuthority>,
}

impl ProjectedOrdinaryRecord {
    /// Borrows the atomically projected claim frontiers.
    #[must_use]
    pub const fn frontiers(&self) -> &ClaimFrontiers {
        &self.frontiers
    }

    /// Returns the complete preferred/cap/resulting floor computation.
    #[must_use]
    pub const fn floor(&self) -> FloorComputation {
        self.floor
    }

    /// Returns exact physical retained occupancy after the floor transition.
    #[must_use]
    pub const fn retained_charge(&self) -> WideResourceVector {
        self.retained_charge
    }

    /// Returns exact post-projection baseline `B`.
    #[must_use]
    pub const fn baseline(&self) -> WideResourceVector {
        self.baseline
    }

    /// Returns the resulting zero-debt closure accounting.
    #[must_use]
    pub const fn accounting(&self) -> ClosureAccounting {
        self.accounting
    }

    /// Returns the exact ordinary required-capacity envelope.
    #[must_use]
    pub const fn required_capacity(&self) -> RequiredCapacityPlan {
        self.required_capacity
    }

    /// Returns the admitted caller-major allocation.
    #[must_use]
    pub const fn order(&self) -> OrderAllocation {
        self.order
    }

    /// Returns the admitted caller-sequence allocation and resulting reserve.
    #[must_use]
    pub const fn sequence(&self) -> SequenceAdmission {
        self.sequence
    }

    /// Returns the shared stage-11 permit selected before frontier mutation.
    #[must_use]
    pub const fn observer_floor(&self) -> ObserverFloorPermit {
        self.observer_floor
    }

    /// Returns the shared stage-12 permit selected before frontier mutation.
    #[must_use]
    pub const fn closure(&self) -> RemainingClosurePermit {
        self.closure
    }

    /// Returns the exact retained caller record.
    #[must_use]
    pub const fn caller_record(&self) -> RetainedCausalRecord {
        self.caller_record
    }

    /// Returns the caller record's exact keyed storage charge.
    #[must_use]
    pub const fn caller_charge(&self) -> RetainedRecordCharge {
        self.caller_charge
    }

    /// Borrows every projected retained causal row in sequence order.
    #[must_use]
    pub fn retained_records(&self) -> &[RetainedCausalRecord] {
        self.frontiers.retained_records()
    }

    /// Borrows one exact keyed charge per projected retained row.
    #[must_use]
    pub fn retained_charges(&self) -> &[RetainedRecordCharge] {
        &self.retained_charges
    }

    /// Borrows newly planned markers in canonical participant order.
    #[must_use]
    pub fn new_marker_candidates(&self) -> &[MarkerCandidateAuthority] {
        &self.new_marker_candidates
    }

    /// Consumes the projection into its updated frontier authority after the
    /// caller has persisted the accompanying rows and accounting atomically.
    #[must_use]
    pub fn into_frontiers(self) -> ClaimFrontiers {
        self.frontiers
    }
}

/// Consuming fixed-point result for one optional ordinary record.
#[derive(Debug, PartialEq, Eq)]
pub enum OrdinaryRecordProjectionDecision {
    /// A pre-owned globally earlier candidate must drain before retry.
    DrainFirst(Box<OrdinaryRecordDrainFirst>),
    /// The ordinary record and exact resulting state passed every gate.
    Projected(Box<ProjectedOrdinaryRecord>),
}

/// Complete factual snapshot consumed by the pure ordinary fixed-point kernel.
///
/// Construction is crate-private.  The eventual frontier wrapper supplies the
/// already validated retained rows, active marker-credit rows, identity ranks,
/// and aggregate ledgers directly from one [`super::super::ClaimFrontiers`].
#[derive(Debug)]
pub(in crate::lifecycle) struct OrdinaryProjectionFacts<'a> {
    pub(in crate::lifecycle) request: RecordAdmissionEnvelope,
    pub(in crate::lifecycle) receiving_binding_epoch: BindingEpoch,
    pub(in crate::lifecycle) encoded_record_charge: ResourceVector,
    pub(in crate::lifecycle) retained_records: &'a [RetainedCausalRecord],
    pub(in crate::lifecycle) retained_charges: &'a [RetainedRecordCharge],
    pub(in crate::lifecycle) active_marker_credit_records: &'a [RetainedCausalRecord],
    pub(in crate::lifecycle) unaccepted_marker_anchors: &'a [DeliverySeq],
    pub(in crate::lifecycle) active_identities: &'a [FrontierParticipant],
    pub(in crate::lifecycle) identity_slot_limit: u64,
    pub(in crate::lifecycle) current_floor: u128,
    pub(in crate::lifecycle) observer_progress: DeliverySeq,
    pub(in crate::lifecycle) order_ledger: OrderLedger,
    pub(in crate::lifecycle) sequence_ledger: SequenceLedger,
    pub(in crate::lifecycle) immutable_candidates: &'a [ImmutableSequenceCandidate],
    pub(in crate::lifecycle) closure_accounting: ClosureAccounting,
    pub(in crate::lifecycle) remaining_recovery_claim: ResourceVector,
    pub(in crate::lifecycle) limits: OrdinaryProjectionLimits,
}

/// Exact globally earlier candidate selected before optional ordinary work.
///
/// This is the factual kernel token.  The consuming frontier wrapper seals it
/// together with ownership of the unchanged [`super::super::ClaimFrontiers`]
/// before exposing a `DrainFirst` decision to a server binding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::lifecycle) struct MandatoryPrefixKey {
    candidate: ImmutableSequenceCandidate,
}

impl MandatoryPrefixKey {
    pub(in crate::lifecycle) const fn candidate(self) -> ImmutableSequenceCandidate {
        self.candidate
    }
}

/// Pure ordinary projection decision before the exact frontier lanes move.
#[derive(Debug, PartialEq, Eq)]
pub(in crate::lifecycle) enum OrdinaryProjectionKernelDecision {
    /// A pre-owned candidate has global precedence and must drain unchanged.
    DrainFirst(Box<MandatoryPrefixKey>),
    /// The caller record and marker fixed point passed every factual check.
    Projected(Box<OrdinaryFixedPointPlan>),
}

/// Complete zero-debt fixed-point result consumed by frontier persistence.
#[derive(Debug, PartialEq, Eq)]
pub(in crate::lifecycle) struct OrdinaryFixedPointPlan {
    floor: FloorComputation,
    retained_charge: WideResourceVector,
    baseline: WideResourceVector,
    resulting_accounting: ClosureAccounting,
    required_capacity: RequiredCapacityPlan,
    order: OrderAllocation,
    sequence: SequenceAdmission,
    caller_record: RetainedCausalRecord,
    caller_charge: RetainedRecordCharge,
    retained_records: Vec<RetainedCausalRecord>,
    retained_charges: Vec<RetainedRecordCharge>,
    marker_candidates: Vec<MarkerCandidateAuthority>,
}

impl OrdinaryFixedPointPlan {
    pub(in crate::lifecycle) const fn floor(&self) -> FloorComputation {
        self.floor
    }

    #[cfg(test)]
    pub(in crate::lifecycle) const fn retained_charge(&self) -> WideResourceVector {
        self.retained_charge
    }

    #[cfg(test)]
    pub(in crate::lifecycle) const fn baseline(&self) -> WideResourceVector {
        self.baseline
    }

    #[cfg(test)]
    pub(in crate::lifecycle) const fn resulting_accounting(&self) -> ClosureAccounting {
        self.resulting_accounting
    }

    pub(in crate::lifecycle) const fn required_capacity(&self) -> RequiredCapacityPlan {
        self.required_capacity
    }

    #[cfg(test)]
    pub(in crate::lifecycle) const fn order(&self) -> OrderAllocation {
        self.order
    }

    #[cfg(test)]
    pub(in crate::lifecycle) const fn sequence(&self) -> SequenceAdmission {
        self.sequence
    }

    #[cfg(test)]
    pub(in crate::lifecycle) const fn caller_record(&self) -> RetainedCausalRecord {
        self.caller_record
    }

    #[cfg(test)]
    pub(in crate::lifecycle) const fn caller_charge(&self) -> RetainedRecordCharge {
        self.caller_charge
    }

    #[cfg(test)]
    pub(in crate::lifecycle) fn retained_records(&self) -> &[RetainedCausalRecord] {
        &self.retained_records
    }

    #[cfg(test)]
    pub(in crate::lifecycle) fn retained_charges(&self) -> &[RetainedRecordCharge] {
        &self.retained_charges
    }

    pub(in crate::lifecycle) fn marker_candidates(&self) -> &[MarkerCandidateAuthority] {
        &self.marker_candidates
    }

    #[allow(clippy::type_complexity)]
    pub(in crate::lifecycle) fn into_parts(
        self,
    ) -> (
        FloorComputation,
        WideResourceVector,
        WideResourceVector,
        ClosureAccounting,
        RequiredCapacityPlan,
        OrderAllocation,
        SequenceAdmission,
        RetainedCausalRecord,
        RetainedRecordCharge,
        Vec<RetainedCausalRecord>,
        Vec<RetainedRecordCharge>,
        Vec<MarkerCandidateAuthority>,
    ) {
        (
            self.floor,
            self.retained_charge,
            self.baseline,
            self.resulting_accounting,
            self.required_capacity,
            self.order,
            self.sequence,
            self.caller_record,
            self.caller_charge,
            self.retained_records,
            self.retained_charges,
            self.marker_candidates,
        )
    }
}

/// Invalid or noncommitting ordinary fixed-point snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OrdinaryProjectionError {
    /// Request and validated frontier name different conversations.
    Conversation,
    /// Ordinary admission cannot run while closure debt owns an edge.
    NonzeroDebt,
    /// A clear closure snapshot retained nonzero recovery occupancy or churn.
    ClearClosureResidue,
    /// The verified sender is absent, detached, or bound to another epoch.
    SenderBinding,
    /// Hard observer progress lies beyond the current durable high watermark.
    ObserverBeyondHighWatermark,
    /// The frontier floor and retained-key suffix disagree.
    RetainedSuffix,
    /// A keyed durability charge does not match its validated causal row.
    RetainedChargeKey {
        /// Zero-based retained-row index selected deterministically.
        index: u64,
    },
    /// One retained row has an impossible zero/multi-entry durability charge.
    RetainedEntryCharge {
        /// Exact offending sequence.
        delivery_seq: DeliverySeq,
        /// Supplied entry charge.
        entries: u64,
    },
    /// A current marker credit does not name one exact retained marker row.
    MarkerCredit,
    /// An unaccepted marker anchor is duplicated or lacks a current credit.
    MarkerAnchor,
    /// Derived current marker-credit count disagrees with durable accounting.
    MarkerCreditAccounting {
        /// Count derived from exact credited records.
        derived: u64,
        /// Count stored in closure accounting.
        stored: u64,
    },
    /// Derived current anchor count disagrees with durable accounting.
    MarkerAnchorAccounting {
        /// Count derived from exact anchor keys.
        derived: u64,
        /// Count stored in closure accounting.
        stored: u64,
    },
    /// Derived retained baseline disagrees with the stored closure baseline.
    BaselineAccounting {
        /// Baseline derived from keyed records and exact current credits.
        derived: WideResourceVector,
        /// Baseline stored in closure accounting.
        stored: WideResourceVector,
    },
    /// Checked-u128 retained or reserve arithmetic failed.
    ArithmeticOverflow {
        /// First component in entry-before-byte order.
        dimension: ResourceDimension,
    },
    /// Caller-major planning failed.
    Order(OrderAdmissionError),
    /// Caller/marker sequence planning failed.
    Sequence(SequenceAdmissionError),
    /// The minimal fitting floor would pass hard observer progress.
    ObserverBackpressure {
        /// Exact minimal capacity floor.
        cap_floor: u128,
        /// Current hard observer progress.
        observer_progress: DeliverySeq,
    },
    /// An unaccepted marker pins the floor before capacity can fit.
    MarkerAnchorCapacity {
        /// Earliest unaccepted marker sequence.
        marker_delivery_seq: DeliverySeq,
        /// Lowest required capacity reachable without crossing that marker.
        required: WideResourceVector,
        /// Configured capacity.
        limit: ResourceVector,
    },
    /// Even the empty post-append retained suffix cannot fit the ordinary envelope.
    Capacity {
        /// Lowest reachable required capacity.
        required: WideResourceVector,
        /// Configured capacity.
        limit: ResourceVector,
    },
    /// The required-capacity helper rejected checked arithmetic.
    RequiredCapacity(RequiredCapacityPlanError),
    /// The derived poststate could not form valid closure accounting.
    ResultingAccounting(ClosureAccountingError),
    /// Existing exact sequence owners could not be relayed behind the caller
    /// record and newly planned marker prefix.
    SequenceRelocation,
    /// Existing exact order owners could not be relayed behind the caller major.
    OrderRelocation,
    /// The shared observer selector disagreed with the successful fixed point.
    ObserverSelectorInvariant,
    /// The shared closure selector disagreed with the successful fixed point.
    ClosureSelectorInvariant,
}

/// Computes the ordinary record's zero-debt floor and marker fixed point.
///
/// This function never accepts a marker count, retained total, proposed floor,
/// or resulting ledger.  All are derived from exact durable facts.  Existing
/// immutable candidates win before optional counter planning.  Successful
/// marker candidates are ordered by ascending permanent participant index and
/// own the caller major's phase-4 suffix.
#[allow(
    clippy::too_many_lines,
    reason = "the fixed point preserves the frozen canonical suboperation order in one pure kernel"
)]
pub(in crate::lifecycle) fn project_ordinary_fixed_point(
    facts: &OrdinaryProjectionFacts<'_>,
) -> Result<OrdinaryProjectionKernelDecision, OrdinaryProjectionError> {
    if let Some(candidate) = facts.immutable_candidates.first().copied() {
        return Ok(OrdinaryProjectionKernelDecision::DrainFirst(Box::new(
            MandatoryPrefixKey { candidate },
        )));
    }
    validate_clear_snapshot(facts)?;
    validate_sender(facts)?;
    validate_retained_suffix(facts)?;
    let marker_state = validate_marker_state(facts)?;
    let current_retained_charge =
        retained_charge_at_floor(facts, facts.current_floor, &marker_state.credits, 0, None)?;
    let current_baseline = retained_baseline_wide(
        current_retained_charge,
        facts.identity_slot_limit,
        usize_to_u64(marker_state.credits.len()),
        facts.limits.marker_max,
    )?;
    if current_baseline != facts.closure_accounting.baseline() {
        return Err(OrdinaryProjectionError::BaselineAccounting {
            derived: current_baseline,
            stored: facts.closure_accounting.baseline(),
        });
    }

    let order = allocate_order(
        OrderAllocatingEnvelope::RecordAdmission(facts.request.clone()),
        facts.order_ledger,
        facts.order_ledger.plan_ordinary_record(),
    )
    .map_err(OrdinaryProjectionError::Order)?;
    let candidate_high_watermark = facts
        .sequence_ledger
        .high_watermark()
        .checked_add(1)
        .ok_or_else(|| {
            OrdinaryProjectionError::Sequence(SequenceAdmissionError::HighWatermarkOverflow {
                high_watermark: facts.sequence_ledger.high_watermark(),
                required_values: 1,
            })
        })?;
    let minimum_member_cursor = facts
        .active_identities
        .iter()
        .map(|participant| participant.cursor())
        .min();
    let base = floor_transition(
        facts.current_floor,
        minimum_member_cursor,
        candidate_high_watermark,
        facts.observer_progress,
        facts.current_floor,
    )
    .resulting_floor;
    let search = search_capacity_floor(facts, &marker_state, candidate_high_watermark, base)?;
    let observer_limit = u128::from(facts.observer_progress) + 1;
    if search.floor > observer_limit {
        return Err(OrdinaryProjectionError::ObserverBackpressure {
            cap_floor: search.floor,
            observer_progress: facts.observer_progress,
        });
    }

    let floor = floor_transition(
        facts.current_floor,
        minimum_member_cursor,
        candidate_high_watermark,
        facts.observer_progress,
        search.floor,
    );
    let marker_count = usize_to_u64(search.marker_participants.len());
    let resulting_sequence = facts
        .sequence_ledger
        .plan_ordinary_record(marker_count)
        .map_err(OrdinaryProjectionError::Sequence)?;
    let sequence = admit_sequence(
        SequenceAllocatingEnvelope::RecordAdmission(facts.request.clone()),
        resulting_sequence,
    )
    .map_err(OrdinaryProjectionError::Sequence)?;
    let caller_order = AdmissionOrder::new(
        order.major(),
        CandidatePhase::OrdinaryRecord,
        facts.request.participant_id,
    );
    let caller_record = RetainedCausalRecord {
        delivery_seq: candidate_high_watermark,
        admission_order: caller_order,
        kind: RetainedCausalRecordKind::OrdinaryRecord {
            participant_index: facts.request.participant_id,
        },
    };
    let caller_charge = RetainedRecordCharge::new(
        candidate_high_watermark,
        caller_order,
        facts.encoded_record_charge,
    );
    let marker_candidates = positioned_markers(
        &search.marker_participants,
        order.major(),
        candidate_high_watermark,
    )?;
    let (retained_records, retained_charges) =
        resulting_retained_rows(facts, floor.resulting_floor, caller_record, caller_charge);
    let resulting_credits = usize_to_u64(search.surviving_credits.len())
        .checked_add(marker_count)
        .ok_or(OrdinaryProjectionError::ArithmeticOverflow {
            dimension: ResourceDimension::Entries,
        })?;
    let resulting_anchors = search
        .surviving_anchor_count
        .checked_add(marker_count)
        .ok_or(OrdinaryProjectionError::ArithmeticOverflow {
            dimension: ResourceDimension::Entries,
        })?;
    let required_capacity = RequiredCapacityPlan::ordinary(
        search.baseline,
        facts.limits.mandatory_bound,
        facts.limits.full_recovery_claim,
    )
    .map_err(OrdinaryProjectionError::RequiredCapacity)?;
    if !zero_debt_admission(
        search.baseline,
        facts.limits.mandatory_bound,
        facts.limits.full_recovery_claim,
        facts.closure_accounting.configured_cap(),
    ) {
        return Err(OrdinaryProjectionError::Capacity {
            required: required_capacity.maximum(),
            limit: facts.closure_accounting.configured_cap(),
        });
    }
    let resulting_accounting = ClosureAccounting::try_new(
        ClosureState::Clear,
        resulting_credits,
        resulting_anchors,
        0,
        0,
        ResourceVector::default(),
        search.baseline,
        facts.closure_accounting.configured_cap(),
        0,
        facts.closure_accounting.episode_churn_limit(),
    )
    .map_err(OrdinaryProjectionError::ResultingAccounting)?;

    Ok(OrdinaryProjectionKernelDecision::Projected(Box::new(
        OrdinaryFixedPointPlan {
            floor,
            retained_charge: search.retained_charge,
            baseline: search.baseline,
            resulting_accounting,
            required_capacity,
            order,
            sequence,
            caller_record,
            caller_charge,
            retained_records,
            retained_charges,
            marker_candidates,
        },
    )))
}

struct MarkerState {
    credits: Vec<MarkerCredit>,
    anchors: Vec<DeliverySeq>,
}

#[derive(Clone, Copy)]
struct MarkerCredit {
    delivery_seq: DeliverySeq,
    participant_index: ParticipantId,
}

struct CapacitySearch {
    floor: u128,
    retained_charge: WideResourceVector,
    baseline: WideResourceVector,
    surviving_credits: Vec<MarkerCredit>,
    surviving_anchor_count: u64,
    marker_participants: Vec<FrontierParticipant>,
}

fn validate_clear_snapshot(
    facts: &OrdinaryProjectionFacts<'_>,
) -> Result<(), OrdinaryProjectionError> {
    if facts.closure_accounting.state() != ClosureState::Clear {
        return Err(OrdinaryProjectionError::NonzeroDebt);
    }
    if facts.remaining_recovery_claim != ResourceVector::default()
        || facts.remaining_recovery_claim != facts.closure_accounting.edge_k_remaining()
        || facts.closure_accounting.episode_churn_used() != 0
    {
        return Err(OrdinaryProjectionError::ClearClosureResidue);
    }
    if facts.observer_progress > facts.sequence_ledger.high_watermark() {
        return Err(OrdinaryProjectionError::ObserverBeyondHighWatermark);
    }
    Ok(())
}

fn validate_sender(facts: &OrdinaryProjectionFacts<'_>) -> Result<(), OrdinaryProjectionError> {
    let sender = facts
        .active_identities
        .iter()
        .find(|participant| participant.participant_index() == facts.request.participant_id);
    if !sender.is_some_and(|participant| {
        participant.binding() == FrontierBinding::Bound(facts.receiving_binding_epoch)
    }) {
        return Err(OrdinaryProjectionError::SenderBinding);
    }
    Ok(())
}

fn validate_retained_suffix(
    facts: &OrdinaryProjectionFacts<'_>,
) -> Result<(), OrdinaryProjectionError> {
    let high_end = u128::from(facts.sequence_ledger.high_watermark()) + 1;
    let expected_len = high_end
        .checked_sub(facts.current_floor)
        .ok_or(OrdinaryProjectionError::RetainedSuffix)?;
    if usize_to_u128(facts.retained_records.len()) != expected_len
        || facts.retained_records.len() != facts.retained_charges.len()
    {
        return Err(OrdinaryProjectionError::RetainedSuffix);
    }
    for (index, (record, charge)) in facts
        .retained_records
        .iter()
        .zip(facts.retained_charges)
        .enumerate()
    {
        let expected_sequence = facts.current_floor + usize_to_u128(index);
        if u128::from(record.delivery_seq) != expected_sequence
            || charge.delivery_seq != record.delivery_seq
            || charge.admission_order != record.admission_order
        {
            return Err(OrdinaryProjectionError::RetainedChargeKey {
                index: usize_to_u64(index),
            });
        }
        if charge.encoded_charge.entries != 1 {
            return Err(OrdinaryProjectionError::RetainedEntryCharge {
                delivery_seq: charge.delivery_seq,
                entries: charge.encoded_charge.entries,
            });
        }
    }
    if facts.encoded_record_charge.entries != 1 {
        return Err(OrdinaryProjectionError::RetainedEntryCharge {
            delivery_seq: facts.sequence_ledger.high_watermark().saturating_add(1),
            entries: facts.encoded_record_charge.entries,
        });
    }
    Ok(())
}

fn validate_marker_state(
    facts: &OrdinaryProjectionFacts<'_>,
) -> Result<MarkerState, OrdinaryProjectionError> {
    if usize_to_u128(facts.active_marker_credit_records.len())
        > u128::from(facts.identity_slot_limit)
    {
        return Err(OrdinaryProjectionError::MarkerCredit);
    }
    let mut credits = Vec::new();
    for credit in facts.active_marker_credit_records {
        let RetainedCausalRecordKind::CompactionMarker {
            participant_index, ..
        } = credit.kind
        else {
            return Err(OrdinaryProjectionError::MarkerCredit);
        };
        if participant_index >= facts.identity_slot_limit
            || !facts
                .active_identities
                .iter()
                .any(|participant| participant.participant_index() == participant_index)
            || credits.iter().any(|current: &MarkerCredit| {
                current.delivery_seq == credit.delivery_seq
                    || current.participant_index == participant_index
            })
            || !facts.retained_records.contains(credit)
        {
            return Err(OrdinaryProjectionError::MarkerCredit);
        }
        credits.push(MarkerCredit {
            delivery_seq: credit.delivery_seq,
            participant_index,
        });
    }
    credits.sort_unstable_by_key(|credit| credit.delivery_seq);
    let derived_credits = usize_to_u64(credits.len());
    if derived_credits != facts.closure_accounting.marker_capacity_credits() {
        return Err(OrdinaryProjectionError::MarkerCreditAccounting {
            derived: derived_credits,
            stored: facts.closure_accounting.marker_capacity_credits(),
        });
    }

    let mut anchors = facts.unaccepted_marker_anchors.to_vec();
    anchors.sort_unstable();
    if anchors.windows(2).any(|pair| pair[0] == pair[1])
        || anchors
            .iter()
            .any(|anchor| !credits.iter().any(|credit| credit.delivery_seq == *anchor))
    {
        return Err(OrdinaryProjectionError::MarkerAnchor);
    }
    let derived_anchors = usize_to_u64(anchors.len());
    if derived_anchors != facts.closure_accounting.marker_anchors() {
        return Err(OrdinaryProjectionError::MarkerAnchorAccounting {
            derived: derived_anchors,
            stored: facts.closure_accounting.marker_anchors(),
        });
    }
    Ok(MarkerState { credits, anchors })
}

#[allow(
    clippy::too_many_lines,
    reason = "each loop iteration evaluates one complete candidate floor in canonical order"
)]
fn search_capacity_floor(
    facts: &OrdinaryProjectionFacts<'_>,
    marker_state: &MarkerState,
    candidate_high_watermark: DeliverySeq,
    mut floor: u128,
) -> Result<CapacitySearch, OrdinaryProjectionError> {
    loop {
        let surviving_credits: Vec<_> = marker_state
            .credits
            .iter()
            .copied()
            .filter(|credit| u128::from(credit.delivery_seq) >= floor)
            .collect();
        let surviving_anchor_count = usize_to_u64(
            marker_state
                .anchors
                .iter()
                .filter(|anchor| u128::from(**anchor) >= floor)
                .count(),
        );
        let marker_participants =
            newly_overtaken(facts.active_identities, &surviving_credits, floor);
        let marker_count = usize_to_u64(marker_participants.len());
        let retained_charge = retained_charge_at_floor(
            facts,
            floor,
            &surviving_credits,
            marker_count,
            Some(candidate_high_watermark),
        )?;
        let resulting_credits = usize_to_u64(surviving_credits.len())
            .checked_add(marker_count)
            .ok_or(OrdinaryProjectionError::ArithmeticOverflow {
                dimension: ResourceDimension::Entries,
            })?;
        let baseline = retained_baseline_wide(
            retained_charge,
            facts.identity_slot_limit,
            resulting_credits,
            facts.limits.marker_max,
        )?;
        if zero_debt_admission(
            baseline,
            facts.limits.mandatory_bound,
            facts.limits.full_recovery_claim,
            facts.closure_accounting.configured_cap(),
        ) {
            if let Some(anchor) = marker_state
                .anchors
                .iter()
                .copied()
                .find(|anchor| u128::from(*anchor) < floor)
            {
                return Err(OrdinaryProjectionError::MarkerAnchorCapacity {
                    marker_delivery_seq: anchor,
                    required: RequiredCapacityPlan::ordinary(
                        baseline,
                        facts.limits.mandatory_bound,
                        facts.limits.full_recovery_claim,
                    )
                    .map_err(OrdinaryProjectionError::RequiredCapacity)?
                    .maximum(),
                    limit: facts.closure_accounting.configured_cap(),
                });
            }
            return Ok(CapacitySearch {
                floor,
                retained_charge,
                baseline,
                surviving_credits,
                surviving_anchor_count,
                marker_participants,
            });
        }

        let next_sequence = facts
            .retained_records
            .iter()
            .map(|record| record.delivery_seq)
            .chain(core::iter::once(candidate_high_watermark))
            .find(|sequence| u128::from(*sequence) >= floor);
        let Some(next_sequence) = next_sequence else {
            let required = RequiredCapacityPlan::ordinary(
                baseline,
                facts.limits.mandatory_bound,
                facts.limits.full_recovery_claim,
            )
            .map_err(OrdinaryProjectionError::RequiredCapacity)?
            .maximum();
            return Err(OrdinaryProjectionError::Capacity {
                required,
                limit: facts.closure_accounting.configured_cap(),
            });
        };
        let next_floor = u128::from(next_sequence) + 1;
        if let Some(anchor) = marker_state
            .anchors
            .iter()
            .copied()
            .find(|anchor| next_floor > u128::from(*anchor))
        {
            let required = RequiredCapacityPlan::ordinary(
                baseline,
                facts.limits.mandatory_bound,
                facts.limits.full_recovery_claim,
            )
            .map_err(OrdinaryProjectionError::RequiredCapacity)?
            .maximum();
            return Err(OrdinaryProjectionError::MarkerAnchorCapacity {
                marker_delivery_seq: anchor,
                required,
                limit: facts.closure_accounting.configured_cap(),
            });
        }
        floor = next_floor;
    }
}

fn retained_charge_at_floor(
    facts: &OrdinaryProjectionFacts<'_>,
    floor: u128,
    current_credits: &[MarkerCredit],
    new_marker_count: u64,
    caller_sequence: Option<DeliverySeq>,
) -> Result<WideResourceVector, OrdinaryProjectionError> {
    let mut charge = WideResourceVector::default();
    for (record, exact) in facts.retained_records.iter().zip(facts.retained_charges) {
        if u128::from(record.delivery_seq) < floor {
            continue;
        }
        let component = if current_credits
            .iter()
            .any(|credit| credit.delivery_seq == record.delivery_seq)
        {
            facts.limits.marker_max
        } else {
            exact.encoded_charge
        };
        charge = checked_add_resource(charge, component)?;
    }
    if caller_sequence.is_some_and(|sequence| u128::from(sequence) >= floor) {
        charge = checked_add_resource(charge, facts.encoded_record_charge)?;
    }
    checked_add_marker_charge(charge, new_marker_count, facts.limits.marker_max)
}

fn retained_baseline_wide(
    retained_charge: WideResourceVector,
    identity_slots: u64,
    marker_credits: u64,
    marker_max: ResourceVector,
) -> Result<WideResourceVector, OrdinaryProjectionError> {
    let uncredited = identity_slots
        .checked_sub(marker_credits)
        .ok_or(OrdinaryProjectionError::MarkerCredit)?;
    checked_add_marker_charge(retained_charge, uncredited, marker_max)
}

fn checked_add_resource(
    current: WideResourceVector,
    increment: ResourceVector,
) -> Result<WideResourceVector, OrdinaryProjectionError> {
    let entries = current
        .entries
        .checked_add(u128::from(increment.entries))
        .ok_or(OrdinaryProjectionError::ArithmeticOverflow {
            dimension: ResourceDimension::Entries,
        })?;
    let bytes = current
        .bytes
        .checked_add(u128::from(increment.bytes))
        .ok_or(OrdinaryProjectionError::ArithmeticOverflow {
            dimension: ResourceDimension::Bytes,
        })?;
    Ok(WideResourceVector::new(entries, bytes))
}

fn checked_add_marker_charge(
    current: WideResourceVector,
    count: u64,
    marker_max: ResourceVector,
) -> Result<WideResourceVector, OrdinaryProjectionError> {
    let entries = u128::from(count)
        .checked_mul(u128::from(marker_max.entries))
        .and_then(|increment| current.entries.checked_add(increment))
        .ok_or(OrdinaryProjectionError::ArithmeticOverflow {
            dimension: ResourceDimension::Entries,
        })?;
    let bytes = u128::from(count)
        .checked_mul(u128::from(marker_max.bytes))
        .and_then(|increment| current.bytes.checked_add(increment))
        .ok_or(OrdinaryProjectionError::ArithmeticOverflow {
            dimension: ResourceDimension::Bytes,
        })?;
    Ok(WideResourceVector::new(entries, bytes))
}

fn newly_overtaken(
    active_identities: &[FrontierParticipant],
    current_credits: &[MarkerCredit],
    floor: u128,
) -> Vec<FrontierParticipant> {
    active_identities
        .iter()
        .copied()
        .filter(|participant| {
            u128::from(participant.cursor()) + 1 < floor
                && !current_credits
                    .iter()
                    .any(|credit| credit.participant_index == participant.participant_index())
        })
        .collect()
}

fn positioned_markers(
    marker_participants: &[FrontierParticipant],
    caller_major: u64,
    caller_sequence: DeliverySeq,
) -> Result<Vec<MarkerCandidateAuthority>, OrdinaryProjectionError> {
    let mut markers = Vec::with_capacity(marker_participants.len());
    for (index, participant) in marker_participants.iter().enumerate() {
        let offset = usize_to_u64(index).checked_add(1).ok_or(
            OrdinaryProjectionError::ArithmeticOverflow {
                dimension: ResourceDimension::Entries,
            },
        )?;
        let delivery_seq =
            caller_sequence
                .checked_add(offset)
                .ok_or(OrdinaryProjectionError::Sequence(
                    SequenceAdmissionError::HighWatermarkOverflow {
                        high_watermark: caller_sequence,
                        required_values: offset,
                    },
                ))?;
        markers.push(MarkerCandidateAuthority {
            delivery_seq,
            admission_order: AdmissionOrder::new(
                caller_major,
                CandidatePhase::CompactionMarker,
                participant.participant_index(),
            ),
            target_binding: participant.binding(),
            provenance: MarkerProvenance::NonProductM,
            current_owner: MarkerSequenceOwner::Marker,
        });
    }
    Ok(markers)
}

fn resulting_retained_rows(
    facts: &OrdinaryProjectionFacts<'_>,
    floor: u128,
    caller_record: RetainedCausalRecord,
    caller_charge: RetainedRecordCharge,
) -> (Vec<RetainedCausalRecord>, Vec<RetainedRecordCharge>) {
    let mut records = Vec::new();
    let mut charges = Vec::new();
    for (record, charge) in facts.retained_records.iter().zip(facts.retained_charges) {
        if u128::from(record.delivery_seq) >= floor {
            records.push(*record);
            charges.push(*charge);
        }
    }
    if u128::from(caller_record.delivery_seq) >= floor {
        records.push(caller_record);
        charges.push(caller_charge);
    }
    (records, charges)
}

fn usize_to_u128(value: usize) -> u128 {
    u64::try_from(value).map_or(u128::MAX, u128::from)
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).map_or(u64::MAX, core::convert::identity)
}
