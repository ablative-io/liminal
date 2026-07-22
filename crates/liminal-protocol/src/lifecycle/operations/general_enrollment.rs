//! Total pre-projection shell for enrollment into a nonempty conversation.
//!
//! This module owns every selector that does not require mutating coupled claim
//! frontiers. It deliberately stops at an opaque projection request: the single
//! consuming `ClaimFrontiers::project_general_enrollment` hook must derive the
//! capacity/floor/marker fixed point and exact owner relocation before a final
//! enrollment commit can exist.

use alloc::{boxed::Box, vec::Vec};

use crate::{
    algebra::ResourceVector,
    wire::{EnrollmentEnvelope, EnrollmentRequest, ServerValue},
};

use super::super::{
    BindingSlotDecision, BindingSlotOccupancy, BindingState, CapacityCounter, ClaimFrontiers,
    ConnectionConversationCapacityCommit, ConnectionConversationTracking, EnrollmentCapacityCommit,
    EnrollmentCapacityCounters, EnrollmentCapacityDecision, EnrollmentLookupResult,
    EnrollmentTokenPhase, ImmutableSequenceCandidate, SemanticConnectionCapacityDecision,
    select_enrollment_binding_slot, select_enrollment_capacity,
    select_semantic_connection_capacity,
};
use super::{OrdinaryProjectionLimits, RetainedRecordCharge};
use crate::wire::ResponseEnvelope;

/// External durability facts and signed limits for the consuming frontier hook.
///
/// Retained rows, cursors, candidates, ledgers, and numeric claim positions are
/// absent: the owned [`ClaimFrontiers`] in [`GeneralEnrollmentPrestate`] is their
/// sole authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GeneralEnrollmentProjectionInput {
    attached_charge: ResourceVector,
    retained_charges: Vec<RetainedRecordCharge>,
    observer_progress: u64,
    closure_accounting: super::super::ClosureAccounting,
    limits: OrdinaryProjectionLimits,
}

impl GeneralEnrollmentProjectionInput {
    /// Captures exact storage charges, observer state, accounting, and signed limits.
    #[must_use]
    pub const fn new(
        attached_charge: ResourceVector,
        retained_charges: Vec<RetainedRecordCharge>,
        observer_progress: u64,
        closure_accounting: super::super::ClosureAccounting,
        limits: OrdinaryProjectionLimits,
    ) -> Self {
        Self {
            attached_charge,
            retained_charges,
            observer_progress,
            closure_accounting,
            limits,
        }
    }

    /// Returns the exact encoded `Attached` row charge.
    #[must_use]
    pub const fn attached_charge(&self) -> ResourceVector {
        self.attached_charge
    }

    /// Borrows one factual keyed charge per currently retained row.
    #[must_use]
    pub fn retained_charges(&self) -> &[RetainedRecordCharge] {
        &self.retained_charges
    }

    /// Returns hard observer progress used by the floor gate.
    #[must_use]
    pub const fn observer_progress(&self) -> u64 {
        self.observer_progress
    }

    /// Returns the unchanged closure-accounting snapshot.
    #[must_use]
    pub const fn closure_accounting(&self) -> super::super::ClosureAccounting {
        self.closure_accounting
    }

    /// Returns the signed marker, mandatory, and recovery bounds.
    #[must_use]
    pub const fn limits(&self) -> OrdinaryProjectionLimits {
        self.limits
    }
}

/// Complete unchanged durable prestate for nonempty enrollment.
#[derive(Debug)]
pub struct GeneralEnrollmentPrestate<'a, EF, V, LF> {
    request: EnrollmentRequest,
    token_phase: EnrollmentTokenPhase<'a, EF, V, LF>,
    lookup_binding: &'a BindingState,
    connection_tracking: ConnectionConversationTracking,
    connection_capacity: CapacityCounter,
    binding_occupancy: BindingSlotOccupancy,
    enrollment_capacity: EnrollmentCapacityCounters,
    frontiers: ClaimFrontiers,
    projection: GeneralEnrollmentProjectionInput,
}

impl<'a, EF, V, LF> GeneralEnrollmentPrestate<'a, EF, V, LF> {
    /// Captures all lookup/capacity facts and the one owned frontier aggregate.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub const fn new(
        request: EnrollmentRequest,
        token_phase: EnrollmentTokenPhase<'a, EF, V, LF>,
        lookup_binding: &'a BindingState,
        connection_tracking: ConnectionConversationTracking,
        connection_capacity: CapacityCounter,
        binding_occupancy: BindingSlotOccupancy,
        enrollment_capacity: EnrollmentCapacityCounters,
        frontiers: ClaimFrontiers,
        projection: GeneralEnrollmentProjectionInput,
    ) -> Self {
        Self {
            request,
            token_phase,
            lookup_binding,
            connection_tracking,
            connection_capacity,
            binding_occupancy,
            enrollment_capacity,
            frontiers,
            projection,
        }
    }

    /// Borrows the exact enrollment request.
    #[must_use]
    pub const fn request(&self) -> &EnrollmentRequest {
        &self.request
    }

    /// Borrows the unchanged coupled frontiers.
    #[must_use]
    pub const fn frontiers(&self) -> &ClaimFrontiers {
        &self.frontiers
    }

    /// Borrows the exact external projection facts.
    #[must_use]
    pub const fn projection(&self) -> &GeneralEnrollmentProjectionInput {
        &self.projection
    }

    /// Returns the unchanged semantic connection counter.
    #[must_use]
    pub const fn connection_capacity(&self) -> CapacityCounter {
        self.connection_capacity
    }
}

/// Exact response paired with the unchanged replayable prestate.
#[derive(Debug)]
pub struct GeneralEnrollmentRefusal<'a, EF, V, LF> {
    response: ServerValue,
    prestate: GeneralEnrollmentPrestate<'a, EF, V, LF>,
}

impl<'a, EF, V, LF> GeneralEnrollmentRefusal<'a, EF, V, LF> {
    /// Borrows the selected wire response.
    #[must_use]
    pub const fn response(&self) -> &ServerValue {
        &self.response
    }

    /// Borrows the byte-for-byte unchanged prestate.
    #[must_use]
    pub const fn prestate(&self) -> &GeneralEnrollmentPrestate<'a, EF, V, LF> {
        &self.prestate
    }

    /// Recovers the response and complete unchanged prestate.
    #[must_use]
    pub fn into_parts(self) -> (ServerValue, GeneralEnrollmentPrestate<'a, EF, V, LF>) {
        (self.response, self.prestate)
    }
}

/// Earlier immutable candidate paired with the unchanged replayable prestate.
#[derive(Debug)]
pub struct GeneralEnrollmentDrainFirst<'a, EF, V, LF> {
    candidate: ImmutableSequenceCandidate,
    prestate: GeneralEnrollmentPrestate<'a, EF, V, LF>,
}

impl<'a, EF, V, LF> GeneralEnrollmentDrainFirst<'a, EF, V, LF> {
    /// Returns the exact lowest immutable candidate.
    #[must_use]
    pub const fn candidate(&self) -> ImmutableSequenceCandidate {
        self.candidate
    }

    /// Borrows the unchanged replayable prestate.
    #[must_use]
    pub const fn prestate(&self) -> &GeneralEnrollmentPrestate<'a, EF, V, LF> {
        &self.prestate
    }

    /// Recovers the candidate and complete unchanged prestate.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        ImmutableSequenceCandidate,
        GeneralEnrollmentPrestate<'a, EF, V, LF>,
    ) {
        (self.candidate, self.prestate)
    }
}

/// Opaque request for the consuming general-enrollment frontier fixed point.
#[derive(Debug)]
pub struct GeneralEnrollmentProjectionRequest<'a, EF, V, LF> {
    prestate: GeneralEnrollmentPrestate<'a, EF, V, LF>,
    connection_capacity: ConnectionConversationCapacityCommit,
    enrollment_capacity: EnrollmentCapacityCommit,
}

impl<'a, EF, V, LF> GeneralEnrollmentProjectionRequest<'a, EF, V, LF> {
    /// Borrows the exact request and unchanged durable projection facts.
    #[must_use]
    pub const fn prestate(&self) -> &GeneralEnrollmentPrestate<'a, EF, V, LF> {
        &self.prestate
    }

    /// Returns the speculative connection-capacity commit.
    #[must_use]
    pub const fn connection_capacity(&self) -> ConnectionConversationCapacityCommit {
        self.connection_capacity
    }

    /// Returns the speculative enrollment-capacity commit.
    #[must_use]
    pub const fn enrollment_capacity(&self) -> EnrollmentCapacityCommit {
        self.enrollment_capacity
    }

    /// Consumes the shell for the crate-owned frontier fixed-point hook.
    #[allow(
        dead_code,
        reason = "ClaimFrontiers::project_general_enrollment consumes this once the frontier hook lands"
    )]
    pub(in crate::lifecycle) fn into_parts(
        self,
    ) -> (
        GeneralEnrollmentPrestate<'a, EF, V, LF>,
        ConnectionConversationCapacityCommit,
        EnrollmentCapacityCommit,
    ) {
        (
            self.prestate,
            self.connection_capacity,
            self.enrollment_capacity,
        )
    }
}

/// Exhaustive nonempty-enrollment decision before the consuming frontier hook.
#[derive(Debug)]
pub enum GeneralEnrollmentDecision<'a, EF, V, LF> {
    /// Exact lookup or capacity response with unchanged state.
    Respond(Box<GeneralEnrollmentRefusal<'a, EF, V, LF>>),
    /// A globally earlier immutable candidate must drain before allocation.
    DrainFirst(Box<GeneralEnrollmentDrainFirst<'a, EF, V, LF>>),
    /// Early selectors passed; the owned frontier fixed point must run next.
    Project(Box<GeneralEnrollmentProjectionRequest<'a, EF, V, LF>>),
}

/// Runs every nonempty-enrollment selector before coupled frontier mutation.
///
/// Lookup and fixed runtime capacities retain their frozen precedence. The
/// lowest immutable candidate is then selected before either allocator or mint
/// closure is invoked. Success returns an opaque request that only the consuming
/// frontier fixed-point hook may complete.
#[must_use]
pub fn prepare_general_enrollment<'a, EF, V, LF>(
    prestate: GeneralEnrollmentPrestate<'a, EF, V, LF>,
) -> GeneralEnrollmentDecision<'a, EF, V, LF> {
    if let Some(response) = enrollment_lookup_response(&prestate) {
        return refused(prestate, response);
    }
    let connection_capacity = match select_semantic_connection_capacity(
        ResponseEnvelope::Enrollment(EnrollmentEnvelope {
            conversation_id: prestate.request.conversation_id,
            enrollment_token: prestate.request.enrollment_token,
        }),
        prestate.connection_tracking,
        prestate.connection_capacity,
    ) {
        SemanticConnectionCapacityDecision::Commit(value) => value,
        SemanticConnectionCapacityDecision::Respond(value) => return refused(prestate, value),
    };
    if let BindingSlotDecision::Respond(value) =
        select_enrollment_binding_slot(&prestate.request, prestate.binding_occupancy)
    {
        return refused(prestate, value);
    }
    let enrollment_capacity =
        match select_enrollment_capacity(&prestate.request, prestate.enrollment_capacity) {
            EnrollmentCapacityDecision::Commit(value) => value,
            EnrollmentCapacityDecision::Respond(value) => return refused(prestate, value),
        };
    if let Some(candidate) = prestate
        .frontiers
        .sequence()
        .immutable_candidates()
        .first()
        .copied()
    {
        return GeneralEnrollmentDecision::DrainFirst(Box::new(GeneralEnrollmentDrainFirst {
            candidate,
            prestate,
        }));
    }
    GeneralEnrollmentDecision::Project(Box::new(GeneralEnrollmentProjectionRequest {
        prestate,
        connection_capacity,
        enrollment_capacity,
    }))
}

fn enrollment_lookup_response<EF, V, LF>(
    prestate: &GeneralEnrollmentPrestate<'_, EF, V, LF>,
) -> Option<ServerValue> {
    match super::super::lookup_enrollment(
        prestate.token_phase,
        prestate.lookup_binding,
        &prestate.request,
    ) {
        EnrollmentLookupResult::Retired(value) => Some(ServerValue::Retired(value)),
        EnrollmentLookupResult::Bound(value) => Some(ServerValue::Bound(value)),
        EnrollmentLookupResult::UnboundReceipt(value) => Some(ServerValue::UnboundReceipt(value)),
        EnrollmentLookupResult::ReceiptExpired(value) => Some(ServerValue::ReceiptExpired(value)),
        EnrollmentLookupResult::EnrollmentKnown(value) => Some(ServerValue::EnrollmentKnown(value)),
        EnrollmentLookupResult::AuthorizedNew => None,
    }
}

fn refused<'a, EF, V, LF>(
    prestate: GeneralEnrollmentPrestate<'a, EF, V, LF>,
    response: ServerValue,
) -> GeneralEnrollmentDecision<'a, EF, V, LF> {
    GeneralEnrollmentDecision::Respond(Box::new(GeneralEnrollmentRefusal { response, prestate }))
}
