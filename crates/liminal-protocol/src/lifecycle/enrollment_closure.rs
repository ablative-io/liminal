//! Protocol-owned closure projection for enrollment.
//!
//! This module first closes the frozen Case 25 base producer: enrollment into
//! an empty, clear conversation. The projection derives every resulting scalar
//! from validated durable ledgers and configuration. It deliberately exposes no
//! constructor for general successor coverage; nonempty histories must not pass
//! a caller-supplied marker count, capacity maximum, edge, or recovery quartet.

use alloc::vec::Vec;

use crate::algebra::{
    BaselineError, MandatoryCapacity, ResourceDimension, ResourceVector, WideResourceVector,
    mandatory_capacity, retained_baseline, zero_debt_admission,
};
use crate::wire::{BindingEpoch, ClosureCheckedEnvelope, Generation, ParticipantIndex};

use super::admission::{
    OrderAdmissionError, OrderClaims, OrderHigh, OrderLedger, ResultingOrderClaims,
    ResultingSequenceState, SequenceAdmissionError, SequenceClaims, SequenceLedger,
};
use super::{
    ClosureAccounting, ClosureAccountingError, ClosureDebt, ClosureState, ObserverProjection,
    RemainingClosureDecision, RequiredCapacityPlan, StoredEdge, check_remaining_closure,
};

/// Maximum accepted frozen churn limit width.
const MAX_CHURN_LIMIT: u64 = u32::MAX as u64;

/// Durable state and signed configuration required by initial enrollment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InitialEnrollmentClosureInput {
    accounting: ClosureAccounting,
    identity_slots: u64,
    mandatory_bound: ResourceVector,
    recovery_claim: ResourceVector,
    marker_max: ResourceVector,
    attached_charge: ResourceVector,
    participant_index: ParticipantIndex,
    binding_epoch: BindingEpoch,
    order: OrderLedger,
    sequence: SequenceLedger,
    physical_floor: u128,
    observer_progress: u64,
}

impl InitialEnrollmentClosureInput {
    /// Captures the persisted empty-conversation facts read by the projector.
    ///
    /// Validation is intentionally deferred to
    /// [`project_initial_enrollment_closure`] so the caller cannot obtain any
    /// authority merely by constructing this factual snapshot.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub const fn new(
        accounting: ClosureAccounting,
        identity_slots: u64,
        mandatory_bound: ResourceVector,
        recovery_claim: ResourceVector,
        marker_max: ResourceVector,
        attached_charge: ResourceVector,
        participant_index: ParticipantIndex,
        binding_epoch: BindingEpoch,
        order: OrderLedger,
        sequence: SequenceLedger,
        physical_floor: u128,
        observer_progress: u64,
    ) -> Self {
        Self {
            accounting,
            identity_slots,
            mandatory_bound,
            recovery_claim,
            marker_max,
            attached_charge,
            participant_index,
            binding_epoch,
            order,
            sequence,
            physical_floor,
            observer_progress,
        }
    }

    /// Replaces the factual binding epoch before projection validation.
    #[must_use]
    pub const fn with_binding_epoch(mut self, binding_epoch: BindingEpoch) -> Self {
        self.binding_epoch = binding_epoch;
        self
    }

    /// Replaces the factual encoded `Attached` charge before validation.
    #[must_use]
    pub const fn with_attached_charge(mut self, attached_charge: ResourceVector) -> Self {
        self.attached_charge = attached_charge;
        self
    }
}

/// Whether one enrollment projection endows the sole recovery quartet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryQuartetStatus {
    /// No anchored delivered-marker recovery branch exists.
    None,
    /// The complete coupled `RS`/`RT`/`RO`/`RA` quartet is endowed once.
    Endowed,
}

/// Planned marker owned by a newly overtaken enrollment participant.
///
/// The initial-enrollment base case always returns an empty set. The type is
/// retained in the projection boundary so later nonempty fixed-point coverage
/// cannot regress to a raw marker count.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlannedEnrollmentMarker {
    participant_index: ParticipantIndex,
    planned_delivery_seq: u64,
}

impl PlannedEnrollmentMarker {
    /// Returns the permanent participant index owning the marker credit.
    #[must_use]
    pub const fn participant_index(self) -> ParticipantIndex {
        self.participant_index
    }

    /// Returns the exact pre-owned sequence value of the marker candidate.
    #[must_use]
    pub const fn planned_delivery_seq(self) -> u64 {
        self.planned_delivery_seq
    }
}

/// Fully derived, persistable initial-enrollment closure projection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InitialEnrollmentClosureProjection {
    current_accounting: ClosureAccounting,
    resulting_accounting: ClosureAccounting,
    resulting_retained_charge: ResourceVector,
    resulting_floor: u128,
    resulting_baseline: WideResourceVector,
    remaining_recovery_claim: ResourceVector,
    capacity: MandatoryCapacity,
    required_capacity: RequiredCapacityPlan,
    recovery_quartet: RecoveryQuartetStatus,
    new_markers: Vec<PlannedEnrollmentMarker>,
    order: OrderLedger,
    sequence: SequenceLedger,
    participant_index: ParticipantIndex,
    identity_slots: u64,
    binding_epoch: BindingEpoch,
}

impl InitialEnrollmentClosureProjection {
    /// Returns the exact closure accounting to persist atomically on success.
    #[must_use]
    pub const fn resulting_closure_accounting(&self) -> ClosureAccounting {
        self.resulting_accounting
    }

    /// Returns the exact typed clear-or-owed closure state.
    #[must_use]
    pub const fn resulting_closure_state(&self) -> ClosureState {
        self.resulting_accounting.state()
    }

    /// Returns the retained closure charge `S'` after the `Attached` append.
    #[must_use]
    pub const fn resulting_retained_charge(&self) -> ResourceVector {
        self.resulting_retained_charge
    }

    /// Returns the reproducible physical floor `F'`.
    #[must_use]
    pub const fn resulting_floor(&self) -> u128 {
        self.resulting_floor
    }

    /// Returns the exact resulting retained baseline `B'`.
    #[must_use]
    pub const fn resulting_baseline(&self) -> WideResourceVector {
        self.resulting_baseline
    }

    /// Returns the exact resulting closure debt.
    #[must_use]
    pub const fn debt(&self) -> WideResourceVector {
        self.capacity.debt
    }

    /// Returns `K_remaining'`, which is full `K` while debt is owed and zero
    /// when the projection remains clear.
    #[must_use]
    pub const fn remaining_recovery_claim(&self) -> ResourceVector {
        self.remaining_recovery_claim
    }

    /// Returns whether the projection endowed the coupled recovery quartet.
    #[must_use]
    pub const fn recovery_quartet(&self) -> RecoveryQuartetStatus {
        self.recovery_quartet
    }

    /// Returns the exact newly created marker candidates.
    #[must_use]
    pub fn new_marker_candidates(&self) -> &[PlannedEnrollmentMarker] {
        &self.new_markers
    }

    /// Returns the componentwise maximum across the complete base successor
    /// coverage used by stage 12.
    #[must_use]
    pub const fn required_capacity(&self) -> RequiredCapacityPlan {
        self.required_capacity
    }

    /// Returns the permanent participant index reserved by this projection.
    #[must_use]
    pub const fn participant_index(&self) -> ParticipantIndex {
        self.participant_index
    }

    /// Returns the validated half-open identity domain used by the projection.
    #[must_use]
    pub const fn identity_slots(&self) -> u64 {
        self.identity_slots
    }

    /// Returns the generation-one binding epoch used by the successor proof.
    #[must_use]
    pub const fn binding_epoch(&self) -> BindingEpoch {
        self.binding_epoch
    }

    /// Returns the unchanged current order ledger consumed by stage 9.
    #[must_use]
    pub const fn current_order(&self) -> OrderLedger {
        self.order
    }

    /// Returns the hard observer progress checked by stage 11.
    #[must_use]
    pub const fn observer_progress(&self) -> u64 {
        0
    }

    /// Produces the sealed enrollment order claims, including a quartet only
    /// when the fixed-point coverage derived one.
    ///
    /// # Errors
    ///
    /// Returns [`OrderAdmissionError`] for claim overflow or an attempted
    /// second recovery quartet.
    pub fn plan_order(&self) -> Result<ResultingOrderClaims, OrderAdmissionError> {
        self.order.plan_enrollment_with_recovery_quartet(matches!(
            self.recovery_quartet,
            RecoveryQuartetStatus::Endowed
        ))
    }

    /// Produces the sealed resulting enrollment sequence state.
    ///
    /// # Errors
    ///
    /// Returns [`SequenceAdmissionError`] for counter/claim overflow or an
    /// attempted second recovery quartet.
    pub fn plan_sequence(&self) -> Result<ResultingSequenceState, SequenceAdmissionError> {
        let marker_count = u64::try_from(self.new_markers.len()).map_err(|_| {
            SequenceAdmissionError::MarkerClaimOverflow {
                markers: self.sequence.claims().markers(),
                new_markers: u64::MAX,
            }
        })?;
        self.sequence.plan_enrollment_with_recovery_quartet(
            marker_count,
            matches!(self.recovery_quartet, RecoveryQuartetStatus::Endowed),
        )
    }

    /// Runs the frozen stage-12 selector against the unchanged prestate.
    ///
    /// Order, sequence, and observer admission must run before the caller
    /// exposes this decision.
    #[must_use]
    pub fn remaining_closure_decision(
        &self,
        request: &ClosureCheckedEnvelope,
    ) -> RemainingClosureDecision {
        check_remaining_closure(
            request,
            self.current_accounting,
            false,
            0,
            self.required_capacity,
        )
    }
}

/// Malformed durable/configuration input for initial enrollment projection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InitialEnrollmentClosureError {
    /// Initial enrollment requires canonical clear closure state.
    ClosureNotClear,
    /// Clear state retained edge-owned claims or recovery occupancy.
    ClearOwnsEdgeResources,
    /// A clear episode must have reset its churn counter.
    ClearChurnNotReset {
        /// Invalid durable churn count.
        used: u64,
    },
    /// Frozen `J` is outside `2..=u32::MAX`.
    ChurnLimit {
        /// Invalid configured value.
        configured: u64,
    },
    /// Initial conversation must have at least one identity slot.
    ZeroIdentitySlots,
    /// Frozen configuration requires `Q == K` componentwise.
    RecoveryClaimDiffersFromMandatoryBound,
    /// Reserved participant index is outside `0..<I`.
    ParticipantIndexOutsideIdentityLimit {
        /// Presented index.
        participant_index: ParticipantIndex,
        /// Configured half-open limit.
        identity_slots: u64,
    },
    /// The empty conversation's monotone allocator must emit index zero.
    InitialParticipantIndexNotZero {
        /// Invalid first allocated index.
        participant_index: ParticipantIndex,
    },
    /// Initial enrollment must bind generation one.
    BindingGeneration {
        /// Invalid binding generation.
        generation: Generation,
    },
    /// Initial order ledger is not empty and claim-free.
    NonemptyOrderLedger,
    /// Initial sequence ledger is not the zero watermark with no claims.
    NonemptySequenceLedger,
    /// Empty log uses exactly `F=1` and `o=0`.
    InitialFloorOrObserver {
        /// Presented physical floor.
        physical_floor: u128,
        /// Presented hard observer progress.
        observer_progress: u64,
    },
    /// Empty durable marker state must have no credits or anchors.
    NonemptyMarkerState {
        /// Durable marker credits.
        credits: u64,
        /// Durable marker anchors.
        anchors: u64,
    },
    /// Durable empty baseline differs from `I * marker_max`.
    BaselineMismatch {
        /// Baseline derived by the protocol.
        derived: WideResourceVector,
        /// Baseline carried by durable accounting.
        durable: WideResourceVector,
    },
    /// The signed startup zero-debt envelope is invalid.
    StartupEnvelope {
        /// First failing resource dimension.
        dimension: ResourceDimension,
    },
    /// Attached charge is outside the mandatory transaction bound.
    AttachedChargeExceedsMandatoryBound {
        /// First failing resource dimension.
        dimension: ResourceDimension,
    },
    /// One `Attached` lifecycle record has exactly one entry.
    AttachedEntryCharge {
        /// Invalid entry charge.
        actual: u64,
    },
    /// Resulting retained charge overflowed its durable u64 representation.
    RetainedChargeOverflow {
        /// First overflowing component.
        dimension: ResourceDimension,
    },
    /// No legal mandatory enrollment poststate exists at the initial floor.
    MandatoryCapacity,
    /// Resulting closure accounting violated its structural invariant.
    ResultingAccounting(ClosureAccountingError),
    /// Baseline derivation rejected an impossible credit count.
    Baseline(BaselineError),
}

/// Projects the complete initial-enrollment closure transition.
///
/// The frozen Case 25 state has no prior rows, members, marker owners, or
/// candidates. Consequently cursor zero is not overtaken at `F'=1`, the marker
/// fixed point is empty, and no anchored DCR branch exists. Positive debt stores
/// `ObserverProjection { through_seq: 1 }`, retains full `K`, and owns no
/// `RS`/`RT`/`RO`/`RA` quartet.
///
/// # Errors
///
/// Returns [`InitialEnrollmentClosureError`] instead of normalizing any
/// malformed durable ordering, accounting, configuration, or initial ledger.
pub fn project_initial_enrollment_closure(
    input: InitialEnrollmentClosureInput,
) -> Result<InitialEnrollmentClosureProjection, InitialEnrollmentClosureError> {
    validate_initial_input(&input)?;

    let initial_baseline = retained_baseline(
        ResourceVector::default(),
        input.identity_slots,
        0,
        input.marker_max,
    )
    .map_err(InitialEnrollmentClosureError::Baseline)?;
    let resulting_retained_charge = input.attached_charge;
    let resulting_baseline = retained_baseline(
        resulting_retained_charge,
        input.identity_slots,
        0,
        input.marker_max,
    )
    .map_err(InitialEnrollmentClosureError::Baseline)?;
    let capacity = mandatory_capacity(
        resulting_baseline,
        input.mandatory_bound,
        input.recovery_claim,
        input.accounting.configured_cap(),
    );
    if !capacity.is_legal() {
        return Err(InitialEnrollmentClosureError::MandatoryCapacity);
    }

    let (state, remaining_recovery_claim, edge_sequence_claims, edge_order_claims) =
        ClosureDebt::new(capacity.debt).map_or_else(
            || (ClosureState::Clear, ResourceVector::default(), 0, 0),
            |debt| {
                (
                    ClosureState::Owed {
                        debt,
                        edge: StoredEdge::ObserverProjection(ObserverProjection::new(1)),
                    },
                    input.recovery_claim,
                    0,
                    0,
                )
            },
        );

    let resulting_accounting = ClosureAccounting::try_new(
        state,
        0,
        0,
        edge_sequence_claims,
        edge_order_claims,
        remaining_recovery_claim,
        resulting_baseline,
        input.accounting.configured_cap(),
        0,
        input.accounting.episode_churn_limit(),
    )
    .map_err(InitialEnrollmentClosureError::ResultingAccounting)?;

    let immediate_required = if capacity.debt.is_zero() {
        checked_sum(
            resulting_baseline,
            input.mandatory_bound,
            input.recovery_claim,
        )?
    } else {
        checked_sum(
            resulting_baseline,
            ResourceVector::default(),
            input.recovery_claim,
        )?
    };
    let clear_successor_required = checked_sum(
        initial_baseline,
        input.mandatory_bound,
        input.recovery_claim,
    )?;
    let required_capacity =
        RequiredCapacityPlan::from_successors(&[immediate_required, clear_successor_required])
            .map_err(|_| InitialEnrollmentClosureError::MandatoryCapacity)?;

    Ok(InitialEnrollmentClosureProjection {
        current_accounting: input.accounting,
        resulting_accounting,
        resulting_retained_charge,
        resulting_floor: 1,
        resulting_baseline,
        remaining_recovery_claim,
        capacity,
        required_capacity,
        recovery_quartet: RecoveryQuartetStatus::None,
        new_markers: Vec::new(),
        order: input.order,
        sequence: input.sequence,
        participant_index: input.participant_index,
        identity_slots: input.identity_slots,
        binding_epoch: input.binding_epoch,
    })
}

fn validate_initial_input(
    input: &InitialEnrollmentClosureInput,
) -> Result<(), InitialEnrollmentClosureError> {
    if input.accounting.state() != ClosureState::Clear {
        return Err(InitialEnrollmentClosureError::ClosureNotClear);
    }
    if input.accounting.edge_sequence_claims() != 0
        || input.accounting.edge_order_position_claims() != 0
        || input.accounting.edge_k_remaining() != ResourceVector::default()
    {
        return Err(InitialEnrollmentClosureError::ClearOwnsEdgeResources);
    }
    if input.accounting.episode_churn_used() != 0 {
        return Err(InitialEnrollmentClosureError::ClearChurnNotReset {
            used: input.accounting.episode_churn_used(),
        });
    }
    let churn_limit = input.accounting.episode_churn_limit();
    if !(2..=MAX_CHURN_LIMIT).contains(&churn_limit) {
        return Err(InitialEnrollmentClosureError::ChurnLimit {
            configured: churn_limit,
        });
    }
    if input.identity_slots == 0 {
        return Err(InitialEnrollmentClosureError::ZeroIdentitySlots);
    }
    if input.mandatory_bound != input.recovery_claim {
        return Err(InitialEnrollmentClosureError::RecoveryClaimDiffersFromMandatoryBound);
    }
    if input.participant_index >= input.identity_slots {
        return Err(
            InitialEnrollmentClosureError::ParticipantIndexOutsideIdentityLimit {
                participant_index: input.participant_index,
                identity_slots: input.identity_slots,
            },
        );
    }
    if input.participant_index != 0 {
        return Err(
            InitialEnrollmentClosureError::InitialParticipantIndexNotZero {
                participant_index: input.participant_index,
            },
        );
    }
    if input.binding_epoch.capability_generation != Generation::ONE {
        return Err(InitialEnrollmentClosureError::BindingGeneration {
            generation: input.binding_epoch.capability_generation,
        });
    }
    validate_initial_ledgers_and_floor(input)?;
    validate_initial_capacity(input)
}

fn validate_initial_ledgers_and_floor(
    input: &InitialEnrollmentClosureInput,
) -> Result<(), InitialEnrollmentClosureError> {
    if input.order.high() != OrderHigh::Empty || input.order.claims() != OrderClaims::default() {
        return Err(InitialEnrollmentClosureError::NonemptyOrderLedger);
    }
    if input.sequence.high_watermark() != 0 || input.sequence.claims() != SequenceClaims::default()
    {
        return Err(InitialEnrollmentClosureError::NonemptySequenceLedger);
    }
    if input.physical_floor != 1 || input.observer_progress != 0 {
        return Err(InitialEnrollmentClosureError::InitialFloorOrObserver {
            physical_floor: input.physical_floor,
            observer_progress: input.observer_progress,
        });
    }
    if input.accounting.marker_capacity_credits() != 0 || input.accounting.marker_anchors() != 0 {
        return Err(InitialEnrollmentClosureError::NonemptyMarkerState {
            credits: input.accounting.marker_capacity_credits(),
            anchors: input.accounting.marker_anchors(),
        });
    }
    Ok(())
}

fn validate_initial_capacity(
    input: &InitialEnrollmentClosureInput,
) -> Result<(), InitialEnrollmentClosureError> {
    let derived_baseline = retained_baseline(
        ResourceVector::default(),
        input.identity_slots,
        0,
        input.marker_max,
    )
    .map_err(InitialEnrollmentClosureError::Baseline)?;
    if derived_baseline != input.accounting.baseline() {
        return Err(InitialEnrollmentClosureError::BaselineMismatch {
            derived: derived_baseline,
            durable: input.accounting.baseline(),
        });
    }
    if let Some(dimension) = startup_envelope_failure(
        derived_baseline,
        input.mandatory_bound,
        input.recovery_claim,
        input.accounting.configured_cap(),
    ) {
        return Err(InitialEnrollmentClosureError::StartupEnvelope { dimension });
    }
    if input.attached_charge.entries != 1 {
        return Err(InitialEnrollmentClosureError::AttachedEntryCharge {
            actual: input.attached_charge.entries,
        });
    }
    if input.attached_charge.entries > input.mandatory_bound.entries {
        return Err(
            InitialEnrollmentClosureError::AttachedChargeExceedsMandatoryBound {
                dimension: ResourceDimension::Entries,
            },
        );
    }
    if input.attached_charge.bytes > input.mandatory_bound.bytes {
        return Err(
            InitialEnrollmentClosureError::AttachedChargeExceedsMandatoryBound {
                dimension: ResourceDimension::Bytes,
            },
        );
    }
    Ok(())
}

const fn startup_envelope_failure(
    baseline: WideResourceVector,
    mandatory_bound: ResourceVector,
    recovery_claim: ResourceVector,
    configured_cap: ResourceVector,
) -> Option<ResourceDimension> {
    if zero_debt_admission(baseline, mandatory_bound, recovery_claim, configured_cap) {
        None
    } else {
        crate::algebra::zero_debt_capacity_failure(
            baseline,
            mandatory_bound,
            recovery_claim,
            configured_cap,
        )
    }
}

fn checked_sum(
    baseline: WideResourceVector,
    middle: ResourceVector,
    last: ResourceVector,
) -> Result<WideResourceVector, InitialEnrollmentClosureError> {
    let Some(entries) = baseline
        .entries
        .checked_add(u128::from(middle.entries))
        .and_then(|value| value.checked_add(u128::from(last.entries)))
    else {
        return Err(InitialEnrollmentClosureError::RetainedChargeOverflow {
            dimension: ResourceDimension::Entries,
        });
    };
    let Some(bytes) = baseline
        .bytes
        .checked_add(u128::from(middle.bytes))
        .and_then(|value| value.checked_add(u128::from(last.bytes)))
    else {
        return Err(InitialEnrollmentClosureError::RetainedChargeOverflow {
            dimension: ResourceDimension::Bytes,
        });
    };
    Ok(WideResourceVector::new(entries, bytes))
}
