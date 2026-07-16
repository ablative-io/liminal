//! Total initial-enrollment operation composition.
//!
//! Token lookup runs before every capacity check. Fresh initial enrollment then
//! composes the shared stage-6, stage-8 through stage-12 selectors and exposes a
//! single opaque commit. Participant-slot allocation is lazy and occurs only at
//! stage 13, so a replay or refusal cannot consume the monotone allocator.

use alloc::boxed::Box;

use crate::wire::{
    AttachSecret, ClosureCheckedEnvelope, EnrollmentEnvelope, EnrollmentRequest,
    OrderAllocatingEnvelope, ResponseEnvelope, SequenceAllocatingEnvelope, ServerValue,
};

use super::super::{
    AllocatedParticipantSlot, AttachedRecordPosition, BindingSlotDecision, BindingSlotOccupancy,
    BindingState, ConnectionConversationCapacityCommit, ConnectionConversationTracking,
    EnrollmentCapacityCommit, EnrollmentCapacityCounters, EnrollmentCapacityDecision,
    EnrollmentCommit, EnrollmentCommitError, EnrollmentCommitParameters, EnrollmentFingerprint,
    EnrollmentLookupResult, EnrollmentTokenPhase, InitialEnrollmentClosureError,
    InitialEnrollmentClosureInput, InitialEnrollmentClosureProjection, ObserverCheckedOperation,
    ObserverFloorDecision, ObserverFloorPermit, OrderAdmissionError, OrderAllocation,
    ParticipantSlotAllocationError, ParticipantSlotAllocatorProof, RemainingClosureDecision,
    RemainingClosurePermit, SemanticConnectionCapacityDecision, SequenceAdmission,
    SequenceAdmissionError, admit_sequence, allocate_order, check_observer_floor,
    commit_enrollment, lookup_enrollment, project_initial_enrollment_closure,
    select_enrollment_binding_slot, select_enrollment_capacity,
    select_semantic_connection_capacity,
};

/// Persisted prestate read by one initial `EnrollmentRequest` attempt.
pub struct InitialEnrollmentOperationInput<'a, EF, V, LF> {
    request: &'a EnrollmentRequest,
    token_phase: EnrollmentTokenPhase<'a, EF, V, LF>,
    lookup_binding: &'a BindingState,
    connection_tracking: ConnectionConversationTracking,
    connection_capacity: super::super::CapacityCounter,
    binding_occupancy: BindingSlotOccupancy,
    enrollment_capacity: EnrollmentCapacityCounters,
    closure: InitialEnrollmentClosureInput,
}

impl<'a, EF, V, LF> InitialEnrollmentOperationInput<'a, EF, V, LF> {
    /// Captures every unchanged persisted fact used by stages 2, 6, and 8-12.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub const fn new(
        request: &'a EnrollmentRequest,
        token_phase: EnrollmentTokenPhase<'a, EF, V, LF>,
        lookup_binding: &'a BindingState,
        connection_tracking: ConnectionConversationTracking,
        connection_capacity: super::super::CapacityCounter,
        binding_occupancy: BindingSlotOccupancy,
        enrollment_capacity: EnrollmentCapacityCounters,
        closure: InitialEnrollmentClosureInput,
    ) -> Self {
        Self {
            request,
            token_phase,
            lookup_binding,
            connection_tracking,
            connection_capacity,
            binding_occupancy,
            enrollment_capacity,
            closure,
        }
    }
}

/// Checked receipt and provenance deadlines derived from one admitted clock read.
///
/// Construction widens the monotonic `u64` clock and both validated `u64` TTLs
/// to `u128` before addition. The provenance deadline therefore cannot precede
/// the receipt deadline, and neither addition can overflow.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReceiptDeadlines {
    receipt_expires_at: u128,
    provenance_expires_at: u128,
}

impl ReceiptDeadlines {
    /// Validates TTLs in frozen configuration precedence and derives deadlines.
    ///
    /// A zero receipt TTL precedes a zero provenance TTL, which precedes the
    /// provenance-order check.
    ///
    /// # Errors
    ///
    /// Returns [`ReceiptDeadlineError`] for the first zero TTL in frozen
    /// configuration order or when provenance is shorter than the receipt.
    pub fn try_from_ttls(
        now_ms: u64,
        attach_receipt_ttl_ms: u64,
        receipt_provenance_ttl_ms: u64,
    ) -> Result<Self, ReceiptDeadlineError> {
        if attach_receipt_ttl_ms == 0 {
            return Err(ReceiptDeadlineError::ZeroAttachReceiptTtl);
        }
        if receipt_provenance_ttl_ms == 0 {
            return Err(ReceiptDeadlineError::ZeroReceiptProvenanceTtl);
        }
        if receipt_provenance_ttl_ms < attach_receipt_ttl_ms {
            return Err(ReceiptDeadlineError::ProvenanceTtlShorterThanReceipt {
                attach_receipt_ttl_ms,
                receipt_provenance_ttl_ms,
            });
        }
        let widened_now = u128::from(now_ms);
        Ok(Self {
            receipt_expires_at: widened_now + u128::from(attach_receipt_ttl_ms),
            provenance_expires_at: widened_now + u128::from(receipt_provenance_ttl_ms),
        })
    }

    /// Returns the checked monotonic receipt deadline.
    #[must_use]
    pub const fn receipt_expires_at(self) -> u128 {
        self.receipt_expires_at
    }

    /// Returns the checked monotonic provenance deadline.
    #[must_use]
    pub const fn provenance_expires_at(self) -> u128 {
        self.provenance_expires_at
    }
}

/// Failure to derive the frozen receipt/provenance deadline pair.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReceiptDeadlineError {
    /// `attach_receipt_ttl_ms` was zero.
    ZeroAttachReceiptTtl,
    /// `receipt_provenance_ttl_ms` was zero after the receipt TTL passed.
    ZeroReceiptProvenanceTtl,
    /// Provenance would expire before the receipt it explains.
    ProvenanceTtlShorterThanReceipt {
        /// Validated nonzero receipt TTL.
        attach_receipt_ttl_ms: u64,
        /// Validated nonzero but insufficient provenance TTL.
        receipt_provenance_ttl_ms: u64,
    },
}

/// Values minted or deadline-derived only after every admission gate passes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InitialEnrollmentCommitValues<F> {
    attach_secret: AttachSecret,
    deadlines: ReceiptDeadlines,
    enrollment_fingerprint: EnrollmentFingerprint<F>,
}

impl<F> InitialEnrollmentCommitValues<F> {
    /// Creates the exact generation-one secret, deadlines, and token mapping.
    #[must_use]
    pub const fn new(
        attach_secret: AttachSecret,
        deadlines: ReceiptDeadlines,
        enrollment_fingerprint: EnrollmentFingerprint<F>,
    ) -> Self {
        Self {
            attach_secret,
            deadlines,
            enrollment_fingerprint,
        }
    }
}

/// Complete atomic initial-enrollment commit.
///
/// Every field is produced by a shared protocol selector. A server binding may
/// persist these values together, but cannot construct this commit directly.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InitialEnrollmentOperationCommit<F> {
    enrollment: EnrollmentCommit<F>,
    connection_capacity: ConnectionConversationCapacityCommit,
    enrollment_capacity: EnrollmentCapacityCommit,
    order: OrderAllocation,
    sequence: SequenceAdmission,
    observer_floor: ObserverFloorPermit,
    closure_permit: Box<RemainingClosurePermit>,
    closure_projection: InitialEnrollmentClosureProjection,
}

impl<F> InitialEnrollmentOperationCommit<F> {
    /// Returns membership, binding, Attached record, and canonical receipt.
    #[must_use]
    pub const fn enrollment(&self) -> &EnrollmentCommit<F> {
        &self.enrollment
    }

    /// Returns the resulting semantic connection-conversation occupancy.
    #[must_use]
    pub const fn connection_capacity(&self) -> ConnectionConversationCapacityCommit {
        self.connection_capacity
    }

    /// Returns all seven resulting identity/receipt/provenance counters.
    #[must_use]
    pub const fn enrollment_capacity(&self) -> EnrollmentCapacityCommit {
        self.enrollment_capacity
    }

    /// Returns the allocated caller major and complete resulting order ledger.
    #[must_use]
    pub const fn order(&self) -> OrderAllocation {
        self.order
    }

    /// Returns the complete admitted sequence ledger.
    #[must_use]
    pub const fn sequence(&self) -> SequenceAdmission {
        self.sequence
    }

    /// Returns the exact stage-11 floor proof.
    #[must_use]
    pub const fn observer_floor(&self) -> ObserverFloorPermit {
        self.observer_floor
    }

    /// Returns the exact stage-12 successor-coverage proof.
    #[must_use]
    pub const fn closure_permit(&self) -> &RemainingClosurePermit {
        &self.closure_permit
    }

    /// Returns the complete persistable floor/retention/debt projection.
    #[must_use]
    pub const fn closure_projection(&self) -> &InitialEnrollmentClosureProjection {
        &self.closure_projection
    }
}

/// Internal invariant fault separated from every wire-visible outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InitialEnrollmentOperationFault {
    /// Durable/configuration closure facts are malformed.
    Closure(InitialEnrollmentClosureError),
    /// Sealed order planning failed without producing wire exhaustion.
    Order(OrderAdmissionError),
    /// Sealed sequence planning failed without producing wire exhaustion.
    Sequence(SequenceAdmissionError),
    /// Lazy monotone allocator rejected its proof.
    SlotAllocation(ParticipantSlotAllocationError),
    /// Allocator and closure projection selected different permanent indices.
    AllocatedParticipantMismatch {
        /// Participant derived by the closure projection.
        expected: u64,
        /// Participant produced by the allocator proof.
        actual: u64,
    },
    /// Allocator and closure projection used different identity domains.
    AllocatedIdentityLimitMismatch {
        /// Validated identity-slot count used by the closure projection.
        expected: u64,
        /// Half-open identity limit bound into the allocator proof.
        actual: u64,
    },
    /// Final membership/binding/receipt construction rejected inconsistent data.
    Commit(EnrollmentCommitError),
}

/// Exhaustive initial-enrollment operation result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InitialEnrollmentOperationDecision<F> {
    /// Exact stable replay or first applicable wire refusal.
    Respond(ServerValue),
    /// Every stage passed and all resulting state may commit atomically.
    Commit(Box<InitialEnrollmentOperationCommit<F>>),
    /// Durable/configuration state violated a protocol invariant.
    Fault(InitialEnrollmentOperationFault),
}

struct InitialEnrollmentCapacityPermits {
    connection: ConnectionConversationCapacityCommit,
    enrollment: EnrollmentCapacityCommit,
}

struct OrderedInitialEnrollment {
    projection: InitialEnrollmentClosureProjection,
    order: OrderAllocation,
}

struct ClosedInitialEnrollment {
    projection: InitialEnrollmentClosureProjection,
    order: OrderAllocation,
    sequence: SequenceAdmission,
    observer_floor: ObserverFloorPermit,
    closure_permit: Box<RemainingClosurePermit>,
}

enum InitialEnrollmentGateFailure {
    Respond(Box<ServerValue>),
    Fault(Box<InitialEnrollmentOperationFault>),
}

impl InitialEnrollmentGateFailure {
    fn respond(value: ServerValue) -> Self {
        Self::Respond(Box::new(value))
    }

    fn fault(value: InitialEnrollmentOperationFault) -> Self {
        Self::Fault(Box::new(value))
    }

    fn into_decision<F>(self) -> InitialEnrollmentOperationDecision<F> {
        match self {
            Self::Respond(value) => InitialEnrollmentOperationDecision::Respond(*value),
            Self::Fault(value) => InitialEnrollmentOperationDecision::Fault(*value),
        }
    }
}

/// Applies frozen stages 2, 6, and 8-13 to initial enrollment.
///
/// Lookup/tombstone/receipt replay precedes semantic connection capacity. Fresh
/// enrollment then checks semantic capacity, binding-slot occupancy, the five
/// reachable runtime-capacity scopes, order, sequence, hard observer retention,
/// remaining closure, and only then invokes the lazy slot allocator and
/// [`commit_enrollment`]. No refusal exposes a partial commit.
#[must_use]
pub fn apply_initial_enrollment<EF, V, LF, F, P, A, M>(
    input: &InitialEnrollmentOperationInput<'_, EF, V, LF>,
    mint_commit_values: M,
    allocate_participant: A,
) -> InitialEnrollmentOperationDecision<F>
where
    P: ParticipantSlotAllocatorProof,
    A: FnOnce() -> Result<AllocatedParticipantSlot<P>, ParticipantSlotAllocationError>,
    M: FnOnce() -> InitialEnrollmentCommitValues<F>,
{
    let envelope = enrollment_envelope(input.request);
    if let Some(response) = initial_enrollment_lookup_response(input) {
        return InitialEnrollmentOperationDecision::Respond(response);
    }
    let capacity = match admit_initial_enrollment_capacity(input, &envelope) {
        Ok(value) => value,
        Err(error) => return error.into_decision(),
    };
    let ordered = match plan_initial_enrollment_order(&input.closure, &envelope) {
        Ok(value) => value,
        Err(error) => return error.into_decision(),
    };
    let closed = match close_initial_enrollment(ordered, &envelope) {
        Ok(value) => value,
        Err(error) => return error.into_decision(),
    };
    commit_initial_enrollment(
        input.request,
        &capacity,
        closed,
        mint_commit_values,
        allocate_participant,
    )
}

fn initial_enrollment_lookup_response<EF, V, LF>(
    input: &InitialEnrollmentOperationInput<'_, EF, V, LF>,
) -> Option<ServerValue> {
    match lookup_enrollment(input.token_phase, input.lookup_binding, input.request) {
        EnrollmentLookupResult::Retired(value) => Some(ServerValue::Retired(value)),
        EnrollmentLookupResult::Bound(value) => Some(ServerValue::Bound(value)),
        EnrollmentLookupResult::UnboundReceipt(value) => Some(ServerValue::UnboundReceipt(value)),
        EnrollmentLookupResult::ReceiptExpired(value) => Some(ServerValue::ReceiptExpired(value)),
        EnrollmentLookupResult::EnrollmentKnown(value) => Some(ServerValue::EnrollmentKnown(value)),
        EnrollmentLookupResult::AuthorizedNew => None,
    }
}

fn admit_initial_enrollment_capacity<EF, V, LF>(
    input: &InitialEnrollmentOperationInput<'_, EF, V, LF>,
    envelope: &EnrollmentEnvelope,
) -> Result<InitialEnrollmentCapacityPermits, InitialEnrollmentGateFailure> {
    let connection = match select_semantic_connection_capacity(
        ResponseEnvelope::Enrollment(envelope.clone()),
        input.connection_tracking,
        input.connection_capacity,
    ) {
        SemanticConnectionCapacityDecision::Commit(value) => value,
        SemanticConnectionCapacityDecision::Respond(value) => {
            return Err(InitialEnrollmentGateFailure::respond(value));
        }
    };
    if let BindingSlotDecision::Respond(value) =
        select_enrollment_binding_slot(input.request, input.binding_occupancy)
    {
        return Err(InitialEnrollmentGateFailure::respond(value));
    }
    let enrollment = match select_enrollment_capacity(input.request, input.enrollment_capacity) {
        EnrollmentCapacityDecision::Commit(value) => value,
        EnrollmentCapacityDecision::Respond(value) => {
            return Err(InitialEnrollmentGateFailure::respond(value));
        }
    };
    Ok(InitialEnrollmentCapacityPermits {
        connection,
        enrollment,
    })
}

fn plan_initial_enrollment_order(
    closure: &InitialEnrollmentClosureInput,
    envelope: &EnrollmentEnvelope,
) -> Result<OrderedInitialEnrollment, InitialEnrollmentGateFailure> {
    let projection = project_initial_enrollment_closure(*closure).map_err(|error| {
        InitialEnrollmentGateFailure::fault(InitialEnrollmentOperationFault::Closure(error))
    })?;
    let order_plan = projection
        .plan_order()
        .map_err(initial_enrollment_order_failure)?;
    let order = allocate_order(
        OrderAllocatingEnvelope::Enrollment(envelope.clone()),
        projection.current_order(),
        order_plan,
    )
    .map_err(initial_enrollment_order_failure)?;
    Ok(OrderedInitialEnrollment { projection, order })
}

fn initial_enrollment_order_failure(error: OrderAdmissionError) -> InitialEnrollmentGateFailure {
    match error {
        OrderAdmissionError::Exhausted(value) => {
            InitialEnrollmentGateFailure::respond(ServerValue::ConversationOrderExhausted(value))
        }
        other => InitialEnrollmentGateFailure::fault(InitialEnrollmentOperationFault::Order(other)),
    }
}

fn close_initial_enrollment(
    ordered: OrderedInitialEnrollment,
    envelope: &EnrollmentEnvelope,
) -> Result<ClosedInitialEnrollment, InitialEnrollmentGateFailure> {
    let sequence_plan = ordered
        .projection
        .plan_sequence()
        .map_err(initial_enrollment_sequence_failure)?;
    let sequence = admit_sequence(
        SequenceAllocatingEnvelope::Enrollment(envelope.clone()),
        sequence_plan,
    )
    .map_err(initial_enrollment_sequence_failure)?;
    let observer_floor = match check_observer_floor(
        ObserverCheckedOperation::Enrollment(envelope.clone()),
        ordered.projection.observer_progress(),
        ordered.projection.resulting_floor(),
    ) {
        ObserverFloorDecision::Eligible(value) => value,
        ObserverFloorDecision::Respond(value) => {
            return Err(InitialEnrollmentGateFailure::respond(
                ServerValue::ObserverBackpressure(value),
            ));
        }
    };
    let closure_permit = match ordered
        .projection
        .remaining_closure_decision(&ClosureCheckedEnvelope::Enrollment(envelope.clone()))
    {
        RemainingClosureDecision::Eligible(value) => value,
        RemainingClosureDecision::Respond(value) => {
            return Err(InitialEnrollmentGateFailure::respond(
                ServerValue::MarkerClosureCapacityExceeded(value),
            ));
        }
    };
    Ok(ClosedInitialEnrollment {
        projection: ordered.projection,
        order: ordered.order,
        sequence,
        observer_floor,
        closure_permit,
    })
}

fn initial_enrollment_sequence_failure(
    error: SequenceAdmissionError,
) -> InitialEnrollmentGateFailure {
    match error {
        SequenceAdmissionError::Exhausted(value) => {
            InitialEnrollmentGateFailure::respond(ServerValue::ConversationSequenceExhausted(value))
        }
        other => {
            InitialEnrollmentGateFailure::fault(InitialEnrollmentOperationFault::Sequence(other))
        }
    }
}

fn commit_initial_enrollment<F, P, A, M>(
    request: &EnrollmentRequest,
    capacity: &InitialEnrollmentCapacityPermits,
    closed: ClosedInitialEnrollment,
    mint_commit_values: M,
    allocate_participant: A,
) -> InitialEnrollmentOperationDecision<F>
where
    P: ParticipantSlotAllocatorProof,
    A: FnOnce() -> Result<AllocatedParticipantSlot<P>, ParticipantSlotAllocationError>,
    M: FnOnce() -> InitialEnrollmentCommitValues<F>,
{
    let participant_slot = match allocate_participant() {
        Ok(value) => value,
        Err(error) => {
            return InitialEnrollmentOperationDecision::Fault(
                InitialEnrollmentOperationFault::SlotAllocation(error),
            );
        }
    };
    if participant_slot.participant_id() != closed.projection.participant_index() {
        return InitialEnrollmentOperationDecision::Fault(
            InitialEnrollmentOperationFault::AllocatedParticipantMismatch {
                expected: closed.projection.participant_index(),
                actual: participant_slot.participant_id(),
            },
        );
    }
    if participant_slot.identity_limit() != closed.projection.identity_slots() {
        return InitialEnrollmentOperationDecision::Fault(
            InitialEnrollmentOperationFault::AllocatedIdentityLimitMismatch {
                expected: closed.projection.identity_slots(),
                actual: participant_slot.identity_limit(),
            },
        );
    }
    let commit_values = mint_commit_values();
    let enrollment = match commit_enrollment(
        request,
        EnrollmentCommitParameters {
            allocated_slot: participant_slot,
            attach_secret: commit_values.attach_secret,
            origin_binding_epoch: closed.projection.binding_epoch(),
            attached_position: AttachedRecordPosition::new(
                closed.order.major(),
                closed.sequence.resulting().high_watermark(),
            ),
            receipt_expires_at: commit_values.deadlines.receipt_expires_at(),
            provenance_expires_at: commit_values.deadlines.provenance_expires_at(),
            enrollment_fingerprint: commit_values.enrollment_fingerprint,
        },
    ) {
        Ok(value) => value,
        Err(error) => {
            return InitialEnrollmentOperationDecision::Fault(
                InitialEnrollmentOperationFault::Commit(error),
            );
        }
    };

    InitialEnrollmentOperationDecision::Commit(Box::new(InitialEnrollmentOperationCommit {
        enrollment,
        connection_capacity: capacity.connection,
        enrollment_capacity: capacity.enrollment,
        order: closed.order,
        sequence: closed.sequence,
        observer_floor: closed.observer_floor,
        closure_permit: closed.closure_permit,
        closure_projection: closed.projection,
    }))
}

const fn enrollment_envelope(request: &EnrollmentRequest) -> EnrollmentEnvelope {
    EnrollmentEnvelope {
        conversation_id: request.conversation_id,
        enrollment_token: request.enrollment_token,
    }
}
