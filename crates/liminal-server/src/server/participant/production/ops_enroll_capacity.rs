//! R-D1 stage-8 capacity pass for the enrollment arm (split from
//! [`super::ops_enroll`] under the 500-code-line lens).

use liminal_protocol::lifecycle::{
    EnrollmentCapacityCounters, EnrollmentCapacityDecision, FreshParticipantCapacityCounter,
    ReceiptDeadlines, select_enrollment_capacity,
};
use liminal_protocol::wire::{
    EnrollmentRequest, EnrollmentResponse, IdentityCapacityExceeded, IdentityCapacityScope,
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
        // before their occupancy is observed (contract R-C0: never a sweep).
        self.prune_expired_provenance(now);
        let identity_conversation_occupied = self.next_participant;
        // The conversation provenance pool is a TRIPWIRE now, not a gate: this
        // occupancy is observed and reported, and refuses nothing.
        self.observe_conversation_provenance_pool(now)?;
        let token = request.enrollment_token.into_bytes();
        let effects = ReservationEffects {
            conversation_id: self.conversation_id,
            identity_reserved: true,
            // Board #37: enrollment creates the receipt body but NO retained
            // provenance. Nothing has yet verified against the secret this
            // receipt mints, so its delivery is unobserved; the fingerprint
            // starts occupying only when the first credential attach proves
            // possession, and that attach reserves it (`ops_attach_capacity`).
            //
            // The provenance SCOPES are still checked below, unchanged: the
            // frozen R-D1 seven-scope order is contract surface and not this
            // lane's to move. Checking a scope this operation no longer fills
            // can only refuse early, never admit past a signed cap.
            inserts: vec![OccupancyEntry {
                expires_at: deadlines.receipt_expires_at(),
                conversation_id: self.conversation_id,
                participant_id: self.next_participant,
                kind: ResourceKind::EnrollmentReceipt,
                token,
            }],
        };
        let tripwires = operation_facts.receipt_limits.shared_pool_tripwires;
        server_capacity.admit(now, tripwires, effects, |server| {
            let counters = match enrollment_scope_counters(
                request,
                operation_facts,
                server,
                identity_conversation_occupied,
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

/// Binds one refusing identity scope to its exact typed wire row.
const fn enrollment_identity_refusal(
    request: &EnrollmentRequest,
    scope: IdentityCapacityScope,
    limit: u64,
    occupied: u64,
) -> EnrollmentResponse {
    EnrollmentResponse::identity_capacity_exceeded(IdentityCapacityExceeded {
        request: enrollment_envelope(request),
        scope,
        limit,
        occupied,
    })
}

/// Builds enrollment's two refusable stage-8 scope counters in the frozen
/// order (identity Server, then identity Conversation) plus the two provably
/// empty per-participant window counters.
///
/// # Lane p0-39: what left this function, and what did not
///
/// The three SHARED receipt scopes are gone. They were the only place an
/// honest third party could be refused by someone else's churn, and they now
/// bound retention through their TTL windows with a reporting tripwire instead
/// of a wall.
///
/// The over-limit arm SURVIVES, for identity only. A configured identity limit
/// lowered beneath restored durable occupancy is still outside the crate's
/// occupancy model, and still refuses with its true numbers rather than
/// admitting past a signed cap — identity slots are out of this lane entirely.
/// The first-full precedence walk it needs is preserved with it.
fn enrollment_scope_counters(
    request: &EnrollmentRequest,
    operation_facts: &OperationFacts,
    server: super::capacity::ServerOccupancy,
    identity_conversation_occupied: u64,
) -> Result<Result<EnrollmentCapacityCounters, EnrollmentResponse>, StateError> {
    let limits = operation_facts.receipt_limits;
    let ordered = [
        (
            limits.identity_server,
            server.identity,
            IdentityCapacityScope::Server,
        ),
        (
            operation_facts.identity_slots,
            identity_conversation_occupied,
            IdentityCapacityScope::Conversation,
        ),
    ];
    let mut counters = Vec::with_capacity(ordered.len());
    // Contract precedence: "The first full scope returns its named
    // IdentityCapacityExceeded; no later occupancy is disclosed." An
    // out-of-model over-limit scope must not answer past an earlier in-model
    // full scope, so the walk remembers the first exactly-full counter it
    // passes and refuses THAT scope when a later over-limit scope ends the
    // walk. When every scope is in model, the crate selector below stays the
    // sole decision owner.
    let mut first_full: Option<(IdentityCapacityScope, u64, u64)> = None;
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
                return Ok(Err(enrollment_identity_refusal(
                    request, scope, limit, occupied,
                )));
            }
        }
    }
    let [identity_server, identity_conversation]: [liminal_protocol::lifecycle::CapacityCounter;
        2] = counters.try_into().map_err(|_| {
        StateError::invariant("enrollment stage-8 scope construction lost a counter")
    })?;
    Ok(Ok(EnrollmentCapacityCounters::new(
        identity_server,
        identity_conversation,
        fresh_participant_counter(limits.live_receipt_participant_window, "live-receipt")?,
        fresh_participant_counter(limits.provenance_participant_window, "provenance")?,
    )))
}
