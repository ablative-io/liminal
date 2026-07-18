//! R-D1 stage-8 capacity pass for the credential-attach arm (split from
//! [`super::ops_attach`] under the 500-code-line lens).

use liminal_protocol::lifecycle::{
    CredentialAttachCapacityCounters, CredentialAttachCapacityDecision, ReceiptDeadlines,
    select_credential_attach_capacity,
};
use liminal_protocol::wire::{
    CredentialAttachRequest, CredentialAttachResponse, ReceiptCapacityScope,
};

use super::barrier::OperationFacts;
use super::capacity::{
    CapacityReservation, OccupancyEntry, ReservationEffects, ResourceKind, ScopeCounter,
    ServerCapacity, Stage8Choice, Stage8Outcome, scope_counter,
};
use super::ops_attach::attach_envelope;
use super::state::{ConversationAuthority, Slot, StateError};

impl ConversationAuthority {
    /// Runs the stage-8 receipt/provenance capacity family for one
    /// authorized credential attach (identity scopes already own their slot,
    /// so the order starts at `LiveReceiptServer`).
    pub(super) fn attach_stage8<'cap>(
        &self,
        request: &CredentialAttachRequest,
        slot: &Slot,
        operation_facts: &OperationFacts,
        server_capacity: &'cap ServerCapacity,
        deadlines: &ReceiptDeadlines,
    ) -> Result<AttachStage8<'cap>, StateError> {
        let now = u128::from(operation_facts.now_ms);
        let limits = operation_facts.receipt_limits;
        let live_receipt_participant_occupied = slot.live_receipt_occupancy(now);
        let provenance_participant_occupied = slot.provenance_occupancy(now)?;
        let provenance_conversation_occupied = self.provenance_occupancy(now)?;
        let token = request.attach_attempt_token.into_bytes();
        let effects = ReservationEffects {
            conversation_id: self.conversation_id,
            identity_reserved: false,
            inserts: vec![
                OccupancyEntry {
                    expires_at: deadlines.receipt_expires_at(),
                    conversation_id: self.conversation_id,
                    participant_id: request.participant_id,
                    kind: ResourceKind::AttachReceipt,
                    token,
                },
                OccupancyEntry {
                    expires_at: deadlines.provenance_expires_at(),
                    conversation_id: self.conversation_id,
                    participant_id: request.participant_id,
                    kind: ResourceKind::AttachProvenance,
                    token,
                },
            ],
        };
        // Receipts this commit will retire early, applied only at confirm.
        let mut retire = Vec::new();
        if let Some(previous) = slot.attach.as_ref() {
            retire.push(OccupancyEntry {
                expires_at: previous.receipt_expires_at,
                conversation_id: self.conversation_id,
                participant_id: request.participant_id,
                kind: ResourceKind::AttachReceipt,
                token: previous.token.into_bytes(),
            });
        }
        if slot.enrollment_receipt_ended.is_none() {
            retire.push(OccupancyEntry {
                expires_at: slot.enrollment_receipt_expires_at,
                conversation_id: self.conversation_id,
                participant_id: request.participant_id,
                kind: ResourceKind::EnrollmentReceipt,
                token: self.enrollment_token_bytes(request.participant_id)?,
            });
        }
        let outcome = server_capacity.admit(now, effects, |server| {
            let counters = match attach_scope_counters(
                request,
                limits,
                server,
                live_receipt_participant_occupied,
                provenance_conversation_occupied,
                provenance_participant_occupied,
            )? {
                Ok(counters) => counters,
                Err(response) => return Ok(Stage8Choice::Refuse(response)),
            };
            // The crate selector owns the in-model full/not-full precedence;
            // its Commit value is carried forward as the ledger reservation.
            match select_credential_attach_capacity(request, counters) {
                CredentialAttachCapacityDecision::Commit(_) => Ok(Stage8Choice::Admit(())),
                CredentialAttachCapacityDecision::Respond(response) => {
                    Ok(Stage8Choice::Refuse(response))
                }
            }
        })?;
        Ok(match outcome {
            Stage8Outcome::Refused(response) => AttachStage8::Refused(response),
            Stage8Outcome::Reserved(reservation, ()) => AttachStage8::Reserved {
                reservation,
                retire,
            },
        })
    }
}

/// Builds credential attach's five stage-8 scope counters in the frozen
/// order (`LiveReceiptServer`, `LiveReceiptParticipant`, `ProvenanceServer`,
/// `ProvenanceConversation`, `ProvenanceParticipant`). A scope whose
/// configured limit was lowered beneath retained durable occupancy is
/// outside the crate's occupancy model (over-limit) and refuses at counter
/// construction rather than admitting past the signed cap — but the
/// contract's first-full precedence still holds: the refusal names the FIRST
/// scope in the frozen order unable to admit one, so an earlier exactly-full
/// scope answers with its own true numbers and no later scope's occupancy is
/// disclosed.
fn attach_scope_counters(
    request: &CredentialAttachRequest,
    limits: super::barrier::ReceiptCapacityLimits,
    server: super::capacity::ServerOccupancy,
    live_receipt_participant_occupied: u64,
    provenance_conversation_occupied: u64,
    provenance_participant_occupied: u64,
) -> Result<Result<CredentialAttachCapacityCounters, CredentialAttachResponse>, StateError> {
    let ordered = [
        (
            limits.live_receipts_server,
            server.live_receipts,
            ReceiptCapacityScope::LiveReceiptServer,
        ),
        (
            limits.live_receipts_per_participant,
            live_receipt_participant_occupied,
            ReceiptCapacityScope::LiveReceiptParticipant,
        ),
        (
            limits.provenance_server,
            server.provenance,
            ReceiptCapacityScope::ProvenanceServer,
        ),
        (
            limits.provenance_per_conversation,
            provenance_conversation_occupied,
            ReceiptCapacityScope::ProvenanceConversation,
        ),
        (
            limits.provenance_per_participant,
            provenance_participant_occupied,
            ReceiptCapacityScope::ProvenanceParticipant,
        ),
    ];
    let mut counters = Vec::with_capacity(ordered.len());
    // Contract precedence (frozen five-scope suborder): "The first full
    // scope returns its named ... ReceiptCapacityExceeded; no later
    // occupancy is disclosed." An out-of-model over-limit scope must not
    // answer past an earlier in-model full scope, so the walk remembers the
    // first exactly-full counter it passes and refuses THAT scope when a
    // later over-limit scope ends the walk. When every scope is in model,
    // the crate selector below stays the sole decision owner.
    let mut first_full: Option<(ReceiptCapacityScope, u64, u64)> = None;
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
                return Ok(Err(CredentialAttachResponse::receipt_capacity_exceeded(
                    attach_envelope(request),
                    scope,
                    limit,
                    occupied,
                )));
            }
        }
    }
    let [lrs, lrp, ps, pc, pp]: [liminal_protocol::lifecycle::CapacityCounter; 5] = counters
        .try_into()
        .map_err(|_| StateError::invariant("attach stage-8 scope construction lost a counter"))?;
    Ok(Ok(CredentialAttachCapacityCounters::new(
        lrs, lrp, ps, pc, pp,
    )))
}

/// Arm-level result of the attach stage-8 pass: a typed refusal, or the
/// live reservation paired with the ledger entries this commit retires.
pub(super) enum AttachStage8<'cap> {
    /// Exact first-full-scope refusal; nothing was reserved.
    Refused(CredentialAttachResponse),
    /// Reserved; confirmed with `retire` after the durable append.
    Reserved {
        /// Stage-8 reservation guard (rolls back unless confirmed).
        reservation: CapacityReservation<'cap>,
        /// Receipt entries the commit ends early.
        retire: Vec<OccupancyEntry>,
    },
}
