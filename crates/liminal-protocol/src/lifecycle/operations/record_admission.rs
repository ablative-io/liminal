//! Total ordinary-record admission after transport and capability negotiation.
//!
//! The operation preserves the frozen selector order and owns the validated
//! [`ClaimFrontiers`] value it projects. A server supplies transport facts,
//! exact durability charges, and signed limits; lookup, capacity, size, global
//! candidate precedence, order/sequence admission, observer retention, closure
//! capacity, and the final record/outcome are all selected by protocol APIs.

use alloc::{boxed::Box, vec::Vec};

use crate::{
    algebra::ResourceVector,
    outcome::CandidatePhase,
    wire::{
        BindingEpoch, ClosureCheckedEnvelope, DeliverySeq, OrderAllocatingEnvelope,
        RecordAdmission, RecordAdmissionEnvelope, RecordCommitted, ResponseEnvelope,
        SequenceAllocatingEnvelope, ServerValue, TransactionOrder,
    },
};

use super::{
    super::{
        AdmissionOrder, BindingRequiredLookupResult, BindingState, CapacityCounter, ClaimFrontiers,
        ClosureAccounting, ClosureState, ConnectionConversationCapacityCommit,
        ConnectionConversationTracking, ImmutableSequenceCandidate, ObserverCheckedOperation,
        ObserverFloorDecision, ObserverFloorPermit, OrderAdmissionError, OrderAllocation,
        ParticipantBindingRequest, PresentedIdentity, RemainingClosureDecision,
        RemainingClosurePermit, RequiredCapacityPlan, RequiredCapacityPlanError,
        SemanticConnectionCapacityDecision, SequenceAdmission, SequenceAdmissionError, StoredEdge,
        admit_sequence, allocate_order, check_observer_floor, check_record_size,
        check_remaining_closure, lookup_binding_required, select_semantic_connection_capacity,
    },
    OrdinaryProjectionError, OrdinaryProjectionLimits, OrdinaryRecordProjectionDecision,
    OrdinaryRecordProjectionInput, ProjectedOrdinaryRecord, RetainedRecordCharge,
};

/// Complete unchanged durable prestate consumed by ordinary admission.
///
/// Order/sequence ledgers, retained rows, marker owners, and participant cursors
/// are intentionally absent as independent fields: the owned [`ClaimFrontiers`]
/// carries them as one validated authority.
#[derive(Debug)]
pub struct RecordAdmissionPrestate<'a, EF, V, LF> {
    request: RecordAdmission,
    presented_identity: PresentedIdentity<'a, EF, V, LF>,
    binding: &'a BindingState,
    receiving_binding_epoch: BindingEpoch,
    connection_tracking: ConnectionConversationTracking,
    connection_capacity: CapacityCounter,
    closure_accounting: ClosureAccounting,
    max_ordinary_record_charge: ResourceVector,
    frontiers: ClaimFrontiers,
    retained_charges: Vec<RetainedRecordCharge>,
    observer_progress: DeliverySeq,
    projection_limits: OrdinaryProjectionLimits,
}

impl<'a, EF, V, LF> RecordAdmissionPrestate<'a, EF, V, LF> {
    /// Captures the exact request, lookup/capacity state, complete validated
    /// frontiers, factual retained charges, observer state, and signed limits.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub const fn new(
        request: RecordAdmission,
        presented_identity: PresentedIdentity<'a, EF, V, LF>,
        binding: &'a BindingState,
        receiving_binding_epoch: BindingEpoch,
        connection_tracking: ConnectionConversationTracking,
        connection_capacity: CapacityCounter,
        closure_accounting: ClosureAccounting,
        max_ordinary_record_charge: ResourceVector,
        frontiers: ClaimFrontiers,
        retained_charges: Vec<RetainedRecordCharge>,
        observer_progress: DeliverySeq,
        projection_limits: OrdinaryProjectionLimits,
    ) -> Self {
        Self {
            request,
            presented_identity,
            binding,
            receiving_binding_epoch,
            connection_tracking,
            connection_capacity,
            closure_accounting,
            max_ordinary_record_charge,
            frontiers,
            retained_charges,
            observer_progress,
            projection_limits,
        }
    }

    /// Borrows the exact payload-bearing request.
    #[must_use]
    pub const fn request(&self) -> &RecordAdmission {
        &self.request
    }

    /// Borrows the unchanged authoritative binding state used by lookup.
    #[must_use]
    pub const fn binding(&self) -> &BindingState {
        self.binding
    }

    /// Returns the unchanged receiving binding epoch.
    #[must_use]
    pub const fn receiving_binding_epoch(&self) -> BindingEpoch {
        self.receiving_binding_epoch
    }

    /// Borrows the unchanged validated claim-frontier aggregate.
    #[must_use]
    pub const fn frontiers(&self) -> &ClaimFrontiers {
        &self.frontiers
    }

    /// Returns the unchanged semantic connection-capacity counter.
    #[must_use]
    pub const fn connection_capacity(&self) -> CapacityCounter {
        self.connection_capacity
    }

    /// Returns the unchanged closure-accounting snapshot.
    #[must_use]
    pub const fn closure_accounting(&self) -> ClosureAccounting {
        self.closure_accounting
    }

    /// Returns the unchanged hard-observer progress.
    #[must_use]
    pub const fn observer_progress(&self) -> DeliverySeq {
        self.observer_progress
    }

    /// Borrows exact keyed durable charges for the retained suffix.
    #[must_use]
    pub fn retained_charges(&self) -> &[RetainedRecordCharge] {
        &self.retained_charges
    }
}

/// Complete unchanged operation state returned by every noncommit decision.
#[derive(Debug)]
pub struct UnchangedRecordAdmission<'a, EF, V, LF> {
    prestate: RecordAdmissionPrestate<'a, EF, V, LF>,
    encoded_record_charge: ResourceVector,
}

impl<'a, EF, V, LF> UnchangedRecordAdmission<'a, EF, V, LF> {
    const fn new(
        prestate: RecordAdmissionPrestate<'a, EF, V, LF>,
        encoded_record_charge: ResourceVector,
    ) -> Self {
        Self {
            prestate,
            encoded_record_charge,
        }
    }

    /// Borrows the exact reusable prestate.
    #[must_use]
    pub const fn prestate(&self) -> &RecordAdmissionPrestate<'a, EF, V, LF> {
        &self.prestate
    }

    /// Returns the unchanged encoded caller-record charge.
    #[must_use]
    pub const fn encoded_record_charge(&self) -> ResourceVector {
        self.encoded_record_charge
    }

    /// Recovers all input needed to replay the pure operation.
    #[must_use]
    pub fn into_parts(self) -> (RecordAdmissionPrestate<'a, EF, V, LF>, ResourceVector) {
        (self.prestate, self.encoded_record_charge)
    }
}

/// Exact wire response paired with the unchanged replayable aggregate.
#[derive(Debug)]
pub struct RecordAdmissionRefusal<'a, EF, V, LF> {
    response: ServerValue,
    unchanged: UnchangedRecordAdmission<'a, EF, V, LF>,
}

impl<'a, EF, V, LF> RecordAdmissionRefusal<'a, EF, V, LF> {
    /// Borrows the selected wire response.
    #[must_use]
    pub const fn response(&self) -> &ServerValue {
        &self.response
    }

    /// Borrows the unchanged replayable aggregate.
    #[must_use]
    pub const fn unchanged(&self) -> &UnchangedRecordAdmission<'a, EF, V, LF> {
        &self.unchanged
    }

    /// Recovers the response and complete unchanged operation state.
    #[must_use]
    pub fn into_parts(self) -> (ServerValue, UnchangedRecordAdmission<'a, EF, V, LF>) {
        (self.response, self.unchanged)
    }
}

/// Earlier immutable candidate paired with the unchanged replayable aggregate.
#[derive(Debug)]
pub struct RecordAdmissionDrainFirst<'a, EF, V, LF> {
    candidate: ImmutableSequenceCandidate,
    unchanged: UnchangedRecordAdmission<'a, EF, V, LF>,
}

impl<'a, EF, V, LF> RecordAdmissionDrainFirst<'a, EF, V, LF> {
    /// Returns the exact lowest immutable candidate selected by the frontier.
    #[must_use]
    pub const fn candidate(&self) -> ImmutableSequenceCandidate {
        self.candidate
    }

    /// Borrows the unchanged replayable aggregate.
    #[must_use]
    pub const fn unchanged(&self) -> &UnchangedRecordAdmission<'a, EF, V, LF> {
        &self.unchanged
    }

    /// Recovers the candidate and complete unchanged operation state.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        ImmutableSequenceCandidate,
        UnchangedRecordAdmission<'a, EF, V, LF>,
    ) {
        (self.candidate, self.unchanged)
    }
}

/// Ordinary record selected for one successful atomic transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommittedOrdinaryRecord {
    request: RecordAdmission,
    admission_order: AdmissionOrder,
    delivery_seq: DeliverySeq,
    encoded_record_charge: ResourceVector,
}

impl CommittedOrdinaryRecord {
    /// Borrows the exact request including its opaque payload.
    #[must_use]
    pub const fn request(&self) -> &RecordAdmission {
        &self.request
    }

    /// Returns the protocol-derived phase-3 admission key.
    #[must_use]
    pub const fn admission_order(&self) -> AdmissionOrder {
        self.admission_order
    }

    /// Returns the assigned gap-free delivery sequence.
    #[must_use]
    pub const fn delivery_seq(&self) -> DeliverySeq {
        self.delivery_seq
    }

    /// Returns the exact encoded charge that passed static size admission.
    #[must_use]
    pub const fn encoded_record_charge(&self) -> ResourceVector {
        self.encoded_record_charge
    }

    const fn new(
        request: RecordAdmission,
        transaction_order: TransactionOrder,
        delivery_seq: DeliverySeq,
        encoded_record_charge: ResourceVector,
    ) -> Self {
        let participant_id = request.participant_id;
        Self {
            request,
            admission_order: AdmissionOrder::new(
                transaction_order,
                CandidatePhase::OrdinaryRecord,
                participant_id,
            ),
            delivery_seq,
            encoded_record_charge,
        }
    }
}

/// Atomic ordinary-record commit selected by every shared admission gate.
#[derive(Debug, PartialEq, Eq)]
pub struct RecordAdmissionCommit {
    outcome: RecordCommitted,
    record: CommittedOrdinaryRecord,
    connection_capacity: ConnectionConversationCapacityCommit,
    projection: Box<ProjectedOrdinaryRecord>,
}

/// Exact successful record-admission parts for one atomic persistence commit.
///
/// The durable conversation writer consumes every field of this value in one
/// atomic transaction. Each field is an owned authority moved out of the
/// selected [`RecordAdmissionCommit`]: nothing here is cloned from, or leaves
/// a second reachable copy inside, the consumed commit. [`ClaimFrontiers`]
/// deliberately implements neither `Clone` nor `Copy`, so the complete
/// resulting frontier authority exists exactly once.
#[derive(Debug)]
pub struct RecordAdmissionPersistenceParts {
    /// Exact payload-bearing response.
    pub outcome: RecordCommitted,
    /// Exact payload-bearing durable caller record.
    pub record: CommittedOrdinaryRecord,
    /// Resulting semantic connection-capacity state.
    pub connection_capacity: ConnectionConversationCapacityCommit,
    /// Admitted caller-major allocation.
    pub order: OrderAllocation,
    /// Admitted caller/marker sequence allocation.
    pub sequence: SequenceAdmission,
    /// Shared observer-floor permit.
    pub observer_floor: ObserverFloorPermit,
    /// Shared remaining-closure permit.
    pub closure: RemainingClosurePermit,
    /// Complete resulting coupled claim frontiers.
    pub frontiers: ClaimFrontiers,
    /// Complete preferred/cap/resulting floor transition.
    pub floor: crate::algebra::FloorComputation,
    /// Exact physical retained occupancy.
    pub retained_charge: crate::algebra::WideResourceVector,
    /// Exact resulting closure baseline.
    pub baseline: crate::algebra::WideResourceVector,
    /// Exact resulting closure accounting.
    pub accounting: ClosureAccounting,
    /// Exact ordinary required-capacity envelope.
    pub required_capacity: RequiredCapacityPlan,
    /// Exact causal caller-row key and kind.
    pub caller_record: super::super::RetainedCausalRecord,
    /// Exact keyed caller-row charge.
    pub caller_charge: RetainedRecordCharge,
    /// One exact keyed charge per retained poststate row.
    pub retained_charges: Vec<RetainedRecordCharge>,
    /// Canonically ordered newly owed markers.
    pub marker_candidates: Vec<super::super::MarkerCandidateAuthority>,
}

impl RecordAdmissionCommit {
    /// Borrows the exact committed wire outcome.
    #[must_use]
    pub const fn outcome(&self) -> &RecordCommitted {
        &self.outcome
    }

    /// Borrows the exact payload-bearing durable record.
    #[must_use]
    pub const fn record(&self) -> &CommittedOrdinaryRecord {
        &self.record
    }

    /// Returns resulting semantic connection capacity.
    #[must_use]
    pub const fn connection_capacity(&self) -> ConnectionConversationCapacityCommit {
        self.connection_capacity
    }

    /// Returns the admitted caller-major allocation.
    #[must_use]
    pub const fn order(&self) -> OrderAllocation {
        self.projection.order()
    }

    /// Returns the admitted caller sequence and complete reserve.
    #[must_use]
    pub const fn sequence(&self) -> SequenceAdmission {
        self.projection.sequence()
    }

    /// Returns the exact stage-11 observer permit.
    #[must_use]
    pub const fn observer_floor(&self) -> ObserverFloorPermit {
        self.projection.observer_floor()
    }

    /// Returns the exact stage-12 closure permit.
    #[must_use]
    pub const fn closure(&self) -> &RemainingClosurePermit {
        &self.projection.closure
    }

    /// Borrows the complete projected frontier/retention/accounting poststate.
    #[must_use]
    pub const fn projection(&self) -> &ProjectedOrdinaryRecord {
        &self.projection
    }

    /// Transfers the exact successful persistence parts without cloning or
    /// dropping any frontier, accounting, row, or marker authority.
    #[must_use]
    pub fn into_persistence_parts(self) -> RecordAdmissionPersistenceParts {
        let ProjectedOrdinaryRecord {
            frontiers,
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
        } = *self.projection;
        RecordAdmissionPersistenceParts {
            outcome: self.outcome,
            record: self.record,
            connection_capacity: self.connection_capacity,
            order,
            sequence,
            observer_floor,
            closure,
            frontiers,
            floor,
            retained_charge,
            baseline,
            accounting,
            required_capacity,
            caller_record,
            caller_charge,
            retained_charges,
            marker_candidates: new_marker_candidates,
        }
    }
}

/// Internal durable/configuration fault, distinct from every wire outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecordAdmissionFault {
    /// The consuming ordinary fixed point rejected inconsistent durable facts.
    Projection(OrdinaryProjectionError),
    /// Nonzero-debt precedence planning failed without wire exhaustion.
    Order(OrderAdmissionError),
    /// Nonzero-debt precedence planning failed without wire exhaustion.
    Sequence(SequenceAdmissionError),
    /// A capacity maximum could not be rebuilt through the shared selector.
    RequiredCapacity(RequiredCapacityPlanError),
    /// A fixed-point refusal failed to reproduce through its shared selector.
    RefusalInvariant,
}

/// Internal fault paired with the unchanged replayable aggregate.
#[derive(Debug)]
pub struct RecordAdmissionFailure<'a, EF, V, LF> {
    fault: RecordAdmissionFault,
    unchanged: UnchangedRecordAdmission<'a, EF, V, LF>,
}

impl<'a, EF, V, LF> RecordAdmissionFailure<'a, EF, V, LF> {
    /// Borrows the selected internal fault.
    #[must_use]
    pub const fn fault(&self) -> &RecordAdmissionFault {
        &self.fault
    }

    /// Borrows the unchanged replayable aggregate.
    #[must_use]
    pub const fn unchanged(&self) -> &UnchangedRecordAdmission<'a, EF, V, LF> {
        &self.unchanged
    }

    /// Recovers the fault and complete unchanged operation state.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        RecordAdmissionFault,
        UnchangedRecordAdmission<'a, EF, V, LF>,
    ) {
        (self.fault, self.unchanged)
    }
}

/// Exhaustive ordinary-record operation result.
#[derive(Debug)]
pub enum RecordAdmissionDecision<'a, EF, V, LF> {
    /// Exact lookup or admission response; no durable mutation is authorized.
    Respond(Box<RecordAdmissionRefusal<'a, EF, V, LF>>),
    /// A globally earlier immutable candidate must drain before retry.
    DrainFirst(Box<RecordAdmissionDrainFirst<'a, EF, V, LF>>),
    /// Every gate passed and all resulting state may commit atomically.
    Commit(Box<RecordAdmissionCommit>),
    /// Durable/configuration state violated an internal invariant.
    Fault(Box<RecordAdmissionFailure<'a, EF, V, LF>>),
}

/// Applies frozen stages 4-12 and constructs the exact phase-13 record commit.
///
/// Binding-required lookup and receiving-epoch validation precede semantic
/// connection capacity and static size. A pre-owned candidate then drains
/// before optional allocation. Order/sequence exhaustion precede observer and
/// closure outcomes. Only the final commit owns changed frontiers or counters.
#[must_use]
#[allow(
    clippy::too_many_lines,
    reason = "the operation keeps the frozen total selector order visible in one function"
)]
pub fn apply_record_admission<EF, V, LF>(
    input: RecordAdmissionPrestate<'_, EF, V, LF>,
    encoded_record_charge: ResourceVector,
) -> RecordAdmissionDecision<'_, EF, V, LF> {
    let envelope = record_envelope(&input.request);

    let lookup_request = ParticipantBindingRequest::RecordAdmission(input.request.clone());
    match lookup_binding_required(
        input.presented_identity,
        input.binding,
        Some(input.receiving_binding_epoch),
        &lookup_request,
    ) {
        BindingRequiredLookupResult::Retired(value) => {
            return refused(input, encoded_record_charge, ServerValue::Retired(value));
        }
        BindingRequiredLookupResult::ParticipantUnknown(value) => {
            return refused(
                input,
                encoded_record_charge,
                ServerValue::ParticipantUnknown(value),
            );
        }
        BindingRequiredLookupResult::StaleAuthority(value) => {
            return refused(
                input,
                encoded_record_charge,
                ServerValue::StaleAuthority(value),
            );
        }
        BindingRequiredLookupResult::NoBinding(value) => {
            return refused(input, encoded_record_charge, ServerValue::NoBinding(value));
        }
        BindingRequiredLookupResult::Authorized { .. } => {}
    }

    let connection_capacity = match select_semantic_connection_capacity(
        ResponseEnvelope::RecordAdmission(envelope.clone()),
        input.connection_tracking,
        input.connection_capacity,
    ) {
        SemanticConnectionCapacityDecision::Commit(value) => value,
        SemanticConnectionCapacityDecision::Respond(value) => {
            return refused(input, encoded_record_charge, value);
        }
    };

    let size = match check_record_size(
        envelope.clone(),
        encoded_record_charge,
        input.max_ordinary_record_charge,
    ) {
        super::super::RecordSizeDecision::Eligible(value) => value,
        super::super::RecordSizeDecision::Respond(value) => {
            return refused(
                input,
                encoded_record_charge,
                ServerValue::RecordTooLarge(value),
            );
        }
    };

    if input.frontiers.sequence().immutable_candidates().is_empty()
        && !matches!(input.closure_accounting.state(), ClosureState::Clear)
    {
        return match nonzero_debt_response(
            &envelope,
            &input.frontiers,
            input.closure_accounting,
            input.observer_progress,
            input.projection_limits,
        ) {
            Ok(response) => refused(input, encoded_record_charge, response),
            Err(operation_fault) => fault(input, encoded_record_charge, operation_fault),
        };
    }

    let RecordAdmissionPrestate {
        request,
        presented_identity,
        binding,
        receiving_binding_epoch,
        connection_tracking,
        connection_capacity: original_connection_capacity,
        closure_accounting,
        max_ordinary_record_charge,
        frontiers,
        retained_charges,
        observer_progress,
        projection_limits,
    } = input;
    let shell = RecordAdmissionProjectionShell {
        request,
        presented_identity,
        binding,
        connection_tracking,
        connection_capacity: original_connection_capacity,
        max_ordinary_record_charge,
    };
    let projection_input = OrdinaryRecordProjectionInput::new(
        envelope.clone(),
        receiving_binding_epoch,
        size.encoded_record_charge(),
        retained_charges,
        observer_progress,
        closure_accounting,
        projection_limits,
    );
    let projected = match frontiers.project_ordinary_record(projection_input) {
        Ok(OrdinaryRecordProjectionDecision::DrainFirst(value)) => {
            let candidate = value.candidate();
            let (frontiers, projection_input) = value.into_unchanged_parts();
            let unchanged = UnchangedRecordAdmission::new(
                shell.rebuild(frontiers, projection_input),
                encoded_record_charge,
            );
            return RecordAdmissionDecision::DrainFirst(Box::new(RecordAdmissionDrainFirst {
                candidate,
                unchanged,
            }));
        }
        Ok(OrdinaryRecordProjectionDecision::Projected(value)) => value,
        Err(failure) => {
            let (frontiers, projection_input, error) = failure.into_parts();
            let prestate = shell.rebuild(frontiers, projection_input);
            return match projection_failure(error, &envelope, closure_accounting) {
                Ok(response) => refused(prestate, encoded_record_charge, response),
                Err(operation_fault) => fault(prestate, encoded_record_charge, operation_fault),
            };
        }
    };

    let order = projected.order();
    let sequence = projected.sequence();
    let delivery_seq = sequence.resulting().high_watermark();
    let record = CommittedOrdinaryRecord::new(
        shell.request,
        order.major(),
        delivery_seq,
        size.encoded_record_charge(),
    );
    RecordAdmissionDecision::Commit(Box::new(RecordAdmissionCommit {
        outcome: RecordCommitted::new(envelope, delivery_seq),
        record,
        connection_capacity,
        projection: projected,
    }))
}

struct RecordAdmissionProjectionShell<'a, EF, V, LF> {
    request: RecordAdmission,
    presented_identity: PresentedIdentity<'a, EF, V, LF>,
    binding: &'a BindingState,
    connection_tracking: ConnectionConversationTracking,
    connection_capacity: CapacityCounter,
    max_ordinary_record_charge: ResourceVector,
}

impl<'a, EF, V, LF> RecordAdmissionProjectionShell<'a, EF, V, LF> {
    fn rebuild(
        self,
        frontiers: ClaimFrontiers,
        projection: OrdinaryRecordProjectionInput,
    ) -> RecordAdmissionPrestate<'a, EF, V, LF> {
        let (
            _envelope,
            receiving_binding_epoch,
            _encoded_record_charge,
            retained_charges,
            observer_progress,
            closure_accounting,
            projection_limits,
        ) = projection.into_parts();
        RecordAdmissionPrestate {
            request: self.request,
            presented_identity: self.presented_identity,
            binding: self.binding,
            receiving_binding_epoch,
            connection_tracking: self.connection_tracking,
            connection_capacity: self.connection_capacity,
            closure_accounting,
            max_ordinary_record_charge: self.max_ordinary_record_charge,
            frontiers,
            retained_charges,
            observer_progress,
            projection_limits,
        }
    }
}

fn refused<EF, V, LF>(
    prestate: RecordAdmissionPrestate<'_, EF, V, LF>,
    encoded_record_charge: ResourceVector,
    response: ServerValue,
) -> RecordAdmissionDecision<'_, EF, V, LF> {
    RecordAdmissionDecision::Respond(Box::new(RecordAdmissionRefusal {
        response,
        unchanged: UnchangedRecordAdmission::new(prestate, encoded_record_charge),
    }))
}

fn fault<EF, V, LF>(
    prestate: RecordAdmissionPrestate<'_, EF, V, LF>,
    encoded_record_charge: ResourceVector,
    operation_fault: RecordAdmissionFault,
) -> RecordAdmissionDecision<'_, EF, V, LF> {
    RecordAdmissionDecision::Fault(Box::new(RecordAdmissionFailure {
        fault: operation_fault,
        unchanged: UnchangedRecordAdmission::new(prestate, encoded_record_charge),
    }))
}

fn nonzero_debt_response(
    envelope: &RecordAdmissionEnvelope,
    frontiers: &ClaimFrontiers,
    accounting: ClosureAccounting,
    observer_progress: DeliverySeq,
    limits: OrdinaryProjectionLimits,
) -> Result<ServerValue, RecordAdmissionFault> {
    let order = match allocate_order(
        OrderAllocatingEnvelope::RecordAdmission(envelope.clone()),
        frontiers.order().ledger(),
        frontiers.order().ledger().plan_ordinary_record(),
    ) {
        Ok(value) => value,
        Err(error) => return order_failure(error),
    };
    let sequence_plan = match frontiers.sequence().ledger().plan_ordinary_record(0) {
        Ok(value) => value,
        Err(error) => return sequence_failure(error),
    };
    if let Err(error) = admit_sequence(
        SequenceAllocatingEnvelope::RecordAdmission(envelope.clone()),
        sequence_plan,
    ) {
        return sequence_failure(error);
    }
    match check_observer_floor(
        ObserverCheckedOperation::RecordAdmission(envelope.clone()),
        observer_progress,
        frontiers.retained_floor(),
    ) {
        ObserverFloorDecision::Eligible(_) => {}
        ObserverFloorDecision::Respond(value) => {
            return Ok(ServerValue::ObserverBackpressure(value));
        }
    }
    let required = match RequiredCapacityPlan::ordinary(
        accounting.baseline(),
        limits.mandatory_bound(),
        accounting.edge_k_remaining(),
    ) {
        Ok(value) => value,
        Err(error) => {
            return Err(RecordAdmissionFault::RequiredCapacity(error));
        }
    };
    let delivered_marker_awaiting_ack = matches!(
        accounting.state(),
        ClosureState::Owed {
            edge: StoredEdge::ParticipantCursorProgress(progress),
            ..
        } if progress.marker_delivery_seq().is_some()
    );
    match check_remaining_closure(
        &ClosureCheckedEnvelope::RecordAdmission(envelope.clone()),
        accounting,
        delivered_marker_awaiting_ack,
        0,
        required,
    ) {
        RemainingClosureDecision::Respond(value) => {
            Ok(ServerValue::MarkerClosureCapacityExceeded(value))
        }
        RemainingClosureDecision::Eligible(_) => {
            let _ = order;
            Err(RecordAdmissionFault::RefusalInvariant)
        }
    }
}

fn projection_failure(
    error: OrdinaryProjectionError,
    envelope: &RecordAdmissionEnvelope,
    accounting: ClosureAccounting,
) -> Result<ServerValue, RecordAdmissionFault> {
    match error {
        OrdinaryProjectionError::Order(error) => order_failure(error),
        OrdinaryProjectionError::Sequence(error) => sequence_failure(error),
        OrdinaryProjectionError::ObserverBackpressure {
            cap_floor,
            observer_progress,
        } => match check_observer_floor(
            ObserverCheckedOperation::RecordAdmission(envelope.clone()),
            observer_progress,
            cap_floor,
        ) {
            ObserverFloorDecision::Respond(value) => Ok(ServerValue::ObserverBackpressure(value)),
            ObserverFloorDecision::Eligible(_) => Err(RecordAdmissionFault::Projection(
                OrdinaryProjectionError::ObserverBackpressure {
                    cap_floor,
                    observer_progress,
                },
            )),
        },
        OrdinaryProjectionError::Capacity { required, .. }
        | OrdinaryProjectionError::MarkerAnchorCapacity { required, .. } => {
            capacity_failure(required, envelope, accounting)
        }
        other => Err(RecordAdmissionFault::Projection(other)),
    }
}

fn capacity_failure(
    required: crate::algebra::WideResourceVector,
    envelope: &RecordAdmissionEnvelope,
    accounting: ClosureAccounting,
) -> Result<ServerValue, RecordAdmissionFault> {
    let required_capacity = match RequiredCapacityPlan::from_successors(&[required]) {
        Ok(value) => value,
        Err(error) => {
            return Err(RecordAdmissionFault::RequiredCapacity(error));
        }
    };
    match check_remaining_closure(
        &ClosureCheckedEnvelope::RecordAdmission(envelope.clone()),
        accounting,
        false,
        0,
        required_capacity,
    ) {
        RemainingClosureDecision::Respond(value) => {
            Ok(ServerValue::MarkerClosureCapacityExceeded(value))
        }
        RemainingClosureDecision::Eligible(_) => Err(RecordAdmissionFault::RefusalInvariant),
    }
}

fn order_failure(error: OrderAdmissionError) -> Result<ServerValue, RecordAdmissionFault> {
    match error {
        OrderAdmissionError::Exhausted(value) => Ok(ServerValue::ConversationOrderExhausted(value)),
        other => Err(RecordAdmissionFault::Order(other)),
    }
}

fn sequence_failure(error: SequenceAdmissionError) -> Result<ServerValue, RecordAdmissionFault> {
    match error {
        SequenceAdmissionError::Exhausted(value) => {
            Ok(ServerValue::ConversationSequenceExhausted(value))
        }
        other => Err(RecordAdmissionFault::Sequence(other)),
    }
}

const fn record_envelope(request: &RecordAdmission) -> RecordAdmissionEnvelope {
    RecordAdmissionEnvelope {
        conversation_id: request.conversation_id,
        participant_id: request.participant_id,
        capability_generation: request.capability_generation,
    }
}
