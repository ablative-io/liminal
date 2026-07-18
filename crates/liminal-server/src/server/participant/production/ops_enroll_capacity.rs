//! R-D1 stage-8 capacity pass for the enrollment arm (split from
//! [`super::ops_enroll`] under the 500-code-line lens).

use liminal_protocol::lifecycle::{
    EnrollmentCapacityCounters, EnrollmentCapacityDecision, FreshParticipantCapacityCounter,
    ReceiptDeadlines, select_enrollment_capacity,
};
use liminal_protocol::wire::{
    EnrollmentReceiptCapacityScope, EnrollmentRequest, EnrollmentResponse,
    IdentityCapacityExceeded, IdentityCapacityScope,
};

use super::barrier::OperationFacts;
use super::capacity::{
    OccupancyEntry, ReservationEffects, ResourceKind, ScopeCounter, ServerCapacity, Stage8Choice,
    Stage8Outcome, scope_counter,
};
use super::ops_enroll::enrollment_envelope;
use super::state::{ConversationAuthority, StateError};

impl ConversationAuthority {
    /// Runs the stage-8 identity/receipt capacity family for one fresh
    /// enrollment: per-conversation occupancies from this authority's own
    /// replayed state, server occupancies from the shared ledger, the
    /// decision through the crate's verified seven-scope selector, and the
    /// reservation applied atomically with the check.
    pub(super) fn enrollment_stage8<'cap>(
        &mut self,
        request: &EnrollmentRequest,
        operation_facts: &OperationFacts,
        server_capacity: &'cap ServerCapacity,
        deadlines: &ReceiptDeadlines,
    ) -> Result<Stage8Outcome<'cap, EnrollmentResponse, EnrollmentCapacityCounters>, StateError>
    {
        let now = u128::from(operation_facts.now_ms);
        // Request-time expiry of this conversation's retained fingerprints
        // before their occupancy is counted.
        self.prune_expired_provenance(now);
        let identity_conversation_occupied = self.next_participant;
        let provenance_conversation_occupied = self.provenance_occupancy(now)?;
        let token = request.enrollment_token.into_bytes();
        let effects = ReservationEffects {
            conversation_id: self.conversation_id,
            identity_reserved: true,
            inserts: vec![
                OccupancyEntry {
                    expires_at: deadlines.receipt_expires_at(),
                    conversation_id: self.conversation_id,
                    participant_id: self.next_participant,
                    kind: ResourceKind::EnrollmentReceipt,
                    token,
                },
                OccupancyEntry {
                    expires_at: deadlines.provenance_expires_at(),
                    conversation_id: self.conversation_id,
                    participant_id: self.next_participant,
                    kind: ResourceKind::EnrollmentProvenance,
                    token,
                },
            ],
        };
        server_capacity.admit(now, effects, |server| {
            let counters = match enrollment_scope_counters(
                request,
                operation_facts,
                server,
                identity_conversation_occupied,
                provenance_conversation_occupied,
            )? {
                Ok(counters) => counters,
                Err(response) => return Ok(Stage8Choice::Refuse(response)),
            };
            // The crate selector owns the in-model full/not-full precedence;
            // its Commit value is carried forward as the ledger reservation.
            match select_enrollment_capacity(request, counters) {
                EnrollmentCapacityDecision::Commit(_) => Ok(Stage8Choice::Admit(counters)),
                EnrollmentCapacityDecision::Respond(response) => Ok(Stage8Choice::Refuse(response)),
            }
        })
    }
}

/// Builds the provably empty per-participant counter for a not-yet-minted
/// identity (contract: both per-participant occupancies are necessarily zero
/// under nonzero limits, so these scopes have no enrollment refusal arm).
fn fresh_participant_counter(
    limit: u64,
    scope: &'static str,
) -> Result<FreshParticipantCapacityCounter, StateError> {
    FreshParticipantCapacityCounter::try_new(limit, 0).map_err(|error| {
        StateError::invariant(format!(
            "validated per-participant {scope} limit rejected: {error:?}"
        ))
    })
}

/// One enrollment stage-8 scope's refusal shape in the frozen order.
#[derive(Clone, Copy)]
enum EnrollmentScope {
    /// `IdentityCapacityExceeded` with the named identity scope.
    Identity(IdentityCapacityScope),
    /// `ReceiptCapacityExceeded` with the named receipt/provenance scope.
    Receipt(EnrollmentReceiptCapacityScope),
}

/// Binds one refusing enrollment scope to its exact typed wire row.
const fn enrollment_scope_refusal(
    request: &EnrollmentRequest,
    scope: EnrollmentScope,
    limit: u64,
    occupied: u64,
) -> EnrollmentResponse {
    match scope {
        EnrollmentScope::Identity(scope) => {
            EnrollmentResponse::identity_capacity_exceeded(IdentityCapacityExceeded {
                request: enrollment_envelope(request),
                scope,
                limit,
                occupied,
            })
        }
        EnrollmentScope::Receipt(scope) => EnrollmentResponse::receipt_capacity_exceeded(
            enrollment_envelope(request),
            scope,
            limit,
            occupied,
        ),
    }
}

/// Builds enrollment's five refusable stage-8 scope counters in the frozen
/// order (identity Server, identity Conversation, `LiveReceiptServer`,
/// `ProvenanceServer`, `ProvenanceConversation`) plus the two provably empty
/// per-participant counters. A scope whose configured limit was lowered
/// beneath retained durable occupancy is outside the crate's occupancy model
/// (over-limit) and refuses at counter construction rather than admitting
/// past the signed cap — but the contract's first-full precedence still
/// holds: the refusal names the FIRST scope in the frozen order unable to
/// admit one, so an earlier exactly-full scope answers with its own true
/// numbers and no later scope's occupancy is disclosed.
fn enrollment_scope_counters(
    request: &EnrollmentRequest,
    operation_facts: &OperationFacts,
    server: super::capacity::ServerOccupancy,
    identity_conversation_occupied: u64,
    provenance_conversation_occupied: u64,
) -> Result<Result<EnrollmentCapacityCounters, EnrollmentResponse>, StateError> {
    let limits = operation_facts.receipt_limits;
    let ordered = [
        (
            limits.identity_server,
            server.identity,
            EnrollmentScope::Identity(IdentityCapacityScope::Server),
        ),
        (
            operation_facts.identity_slots,
            identity_conversation_occupied,
            EnrollmentScope::Identity(IdentityCapacityScope::Conversation),
        ),
        (
            limits.live_receipts_server,
            server.live_receipts,
            EnrollmentScope::Receipt(EnrollmentReceiptCapacityScope::LiveReceiptServer),
        ),
        (
            limits.provenance_server,
            server.provenance,
            EnrollmentScope::Receipt(EnrollmentReceiptCapacityScope::ProvenanceServer),
        ),
        (
            limits.provenance_per_conversation,
            provenance_conversation_occupied,
            EnrollmentScope::Receipt(EnrollmentReceiptCapacityScope::ProvenanceConversation),
        ),
    ];
    let mut counters = Vec::with_capacity(ordered.len());
    // Contract precedence (frozen seven-scope suborder): "The first full
    // scope returns its named IdentityCapacityExceeded or
    // ReceiptCapacityExceeded; no later occupancy is disclosed." An
    // out-of-model over-limit scope must not answer past an earlier in-model
    // full scope, so the walk remembers the first exactly-full counter it
    // passes and refuses THAT scope when a later over-limit scope ends the
    // walk. When every scope is in model, the crate selector below stays the
    // sole decision owner.
    let mut first_full: Option<(EnrollmentScope, u64, u64)> = None;
    for (limit, occupied, scope) in ordered {
        match scope_counter(limit, occupied)? {
            ScopeCounter::Valid(counter) => {
                if first_full.is_none() && counter.is_full() {
                    first_full = Some((scope, counter.limit(), counter.occupied()));
                }
                counters.push(counter);
            }
            ScopeCounter::OverLimit { limit, occupied } => {
                let (scope, limit, occupied) = first_full.unwrap_or((scope, limit, occupied));
                return Ok(Err(enrollment_scope_refusal(
                    request, scope, limit, occupied,
                )));
            }
        }
    }
    let [
        identity_server,
        identity_conversation,
        live_receipt_server,
        provenance_server,
        provenance_conversation,
    ]: [liminal_protocol::lifecycle::CapacityCounter; 5] = counters.try_into().map_err(|_| {
        StateError::invariant("enrollment stage-8 scope construction lost a counter")
    })?;
    Ok(Ok(EnrollmentCapacityCounters::new(
        identity_server,
        identity_conversation,
        live_receipt_server,
        fresh_participant_counter(limits.live_receipts_per_participant, "live-receipt")?,
        provenance_server,
        provenance_conversation,
        fresh_participant_counter(limits.provenance_per_participant, "provenance")?,
    )))
}
