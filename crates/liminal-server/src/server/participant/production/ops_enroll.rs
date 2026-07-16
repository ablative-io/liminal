//! Enrollment arm of the production handler.
//!
//! Classification flows through the shared enrollment lookup, commits through
//! the crate's typed enrollment transition, mints the shell event through the
//! A3 aggregate commit, and answers through the request-bound response
//! authority. No lifecycle outcome is constructed here.
//!
//! Error contract: any [`StateError`] leaves durable state untouched (nothing
//! is published before the append succeeds) but may have consumed in-memory
//! authority. The handler therefore discards the whole in-memory conversation
//! owner on error and cold-replays durable reality on the next touch — the
//! same crash-consistency model the aggregate barrier is built for.

use liminal_protocol::lifecycle::{
    AggregateOperationDecision, AllocatedParticipantSlot, AttachedRecordPosition,
    BindingSlotDecision, DetachCell, EnrollmentCommitParameters, EnrollmentFingerprint,
    EnrollmentLiveReceipt, EnrollmentLookupResult, EnrollmentTokenPhase,
    ParticipantSlotAllocatorProof, ResolvedIdentity, commit_enrollment, decide_enrolled_operation,
    lookup_enrollment, select_enrollment_binding_slot,
};
use liminal_protocol::wire::{
    AttachSecret, BindingEpoch, EnrollBound, EnrollmentRequest, EnrollmentResponse, Generation,
    ServerValue,
};

use super::barrier::{CommitMode, OperationFacts, commit_through_barrier};
use super::facts::{self, Digest};
use super::log::{StoredEnrollmentAllocation, StoredEnrollmentRequest, StoredOperation};
use super::state::{ConversationAuthority, DurableAppend, Slot, StateError};

/// Server-owned participant-slot allocation proof for one enrollment.
#[derive(Clone, Copy, Debug)]
struct ServerSlotProof {
    conversation_id: u64,
    participant_id: u64,
    identity_limit: u64,
}

impl ParticipantSlotAllocatorProof for ServerSlotProof {
    fn conversation_id(&self) -> u64 {
        self.conversation_id
    }

    fn participant_index(&self) -> u64 {
        self.participant_id
    }

    fn identity_limit(&self) -> u64 {
        self.identity_limit
    }
}

impl ConversationAuthority {
    /// Applies one enrollment request end to end.
    ///
    /// Every refusal (token replay, binding-slot occupancy) classifies over
    /// the replayed authority WITHOUT touching durable state; the durable
    /// shell genesis is minted only on the authorized-new arm, immediately
    /// before the enrollment's own committing append — a refused request on a
    /// never-seen conversation id leaves the durable store byte-identical.
    pub(super) fn apply_enrollment(
        &mut self,
        request: &EnrollmentRequest,
        operation_facts: &OperationFacts,
        appender: &dyn DurableAppend,
    ) -> Result<ServerValue, StateError> {
        let token_bytes = request.enrollment_token.into_bytes();
        if let Some(participant_id) = self.tokens.get(&token_bytes).copied() {
            let slot = self.slots.get(&participant_id).ok_or_else(|| {
                StateError::invariant("enrollment token maps to a missing participant slot")
            })?;
            return enrollment_replay_response(slot, request, operation_facts);
        }
        if let BindingSlotDecision::Respond(response) = select_enrollment_binding_slot(
            request,
            self.binding_slot_occupancy(operation_facts.receiving_incarnation),
        ) {
            return Ok(response.into_server_value());
        }

        let deadlines = operation_facts.deadlines()?;
        // The one conversation-creating arm: genesis is durable exactly when
        // an authorized enrollment is about to append its own entry.
        self.ensure_genesis(appender)?;
        let (attached_order, attached_seq) = self.allocate_position()?;
        let allocation = StoredEnrollmentAllocation {
            participant_id: self.next_participant,
            identity_limit: operation_facts.identity_slots,
            attach_secret: facts::mint_secret_bytes()?,
            origin_epoch: BindingEpoch::new(operation_facts.receiving_incarnation, Generation::ONE)
                .into(),
            attached_order,
            attached_seq,
            receipt_expires_at: deadlines.receipt_expires_at().into(),
            provenance_expires_at: deadlines.provenance_expires_at().into(),
            enrollment_fingerprint: facts::enrollment_fingerprint(request.enrollment_token),
        };
        let outcome = self.enroll_commit(request, &allocation, CommitMode::Live(appender))?;
        Ok(EnrollmentResponse::enroll_bound(outcome).into_server_value())
    }

    /// Replays one committed enrollment entry from its stored inputs.
    pub(super) fn replay_enrolled(
        &mut self,
        request: StoredEnrollmentRequest,
        allocation: &StoredEnrollmentAllocation,
        stored_event: &[u8],
        sequence: u64,
    ) -> Result<(), StateError> {
        let request = request.to_request();
        self.enroll_commit(
            &request,
            allocation,
            CommitMode::Replay {
                stored_event,
                sequence,
            },
        )?;
        Ok(())
    }

    /// Shared enrollment commit core for the live and replay paths.
    fn enroll_commit(
        &mut self,
        request: &EnrollmentRequest,
        allocation: &StoredEnrollmentAllocation,
        mode: CommitMode<'_>,
    ) -> Result<EnrollBound, StateError> {
        let allocated_slot = AllocatedParticipantSlot::from_allocator(ServerSlotProof {
            conversation_id: request.conversation_id,
            participant_id: allocation.participant_id,
            identity_limit: allocation.identity_limit,
        })
        .map_err(|error| {
            StateError::invariant(format!("participant slot allocation rejected: {error:?}"))
        })?;
        let committed = commit_enrollment(
            request,
            EnrollmentCommitParameters {
                allocated_slot,
                attach_secret: AttachSecret::new(allocation.attach_secret),
                origin_binding_epoch: allocation.origin_epoch.to_epoch()?,
                attached_position: AttachedRecordPosition::new(
                    allocation.attached_order,
                    allocation.attached_seq,
                ),
                receipt_expires_at: allocation.receipt_expires_at.get(),
                provenance_expires_at: allocation.provenance_expires_at.get(),
                enrollment_fingerprint: EnrollmentFingerprint::new(
                    allocation.enrollment_fingerprint,
                ),
            },
        )
        .map_err(|error| {
            StateError::invariant(format!("protocol enrollment transition failed: {error:?}"))
        })?;
        let shell = self.take_shell()?;
        let barrier = match decide_enrolled_operation(shell, committed) {
            AggregateOperationDecision::Commit(barrier) => barrier,
            AggregateOperationDecision::Refused(refusal) => {
                return Err(StateError::ShellRefused {
                    reason: refusal.reason(),
                });
            }
        };
        let make_operation = |event: Vec<u8>| StoredOperation::Enrolled {
            request: request.into(),
            allocation: *allocation,
            event,
        };
        let (shell, committed) =
            commit_through_barrier(barrier, mode, self.next_log_sequence, &make_operation)?;
        self.shell = Some(shell);
        self.advance_log_head()?;
        let outcome = committed.outcome.clone();
        self.slots.insert(
            allocation.participant_id,
            Slot {
                member: committed.member,
                binding: committed.binding_state,
                cell: DetachCell::default(),
                enrollment_receipt: EnrollmentLiveReceipt::from_commit(outcome.clone()),
                enrollment_outcome: committed.outcome,
                enrollment_receipt_expires_at: allocation.receipt_expires_at.get(),
                attach: None,
                attach_provenance: std::collections::BTreeMap::new(),
                attach_secret: AttachSecret::new(allocation.attach_secret),
                exact_detach_token: None,
            },
        );
        self.tokens.insert(
            request.enrollment_token.into_bytes(),
            allocation.participant_id,
        );
        self.next_participant = allocation
            .participant_id
            .checked_add(1)
            .ok_or(StateError::AllocationExhausted {
                domain: "participant index",
            })?
            .max(self.next_participant);
        self.observe_replayed_position(allocation.attached_order, allocation.attached_seq);
        Ok(outcome)
    }
}

/// Builds the enrollment replay/known response for a mapped token.
fn enrollment_replay_response(
    slot: &Slot,
    request: &EnrollmentRequest,
    operation_facts: &OperationFacts,
) -> Result<ServerValue, StateError> {
    let now = u128::from(operation_facts.now_ms);
    // The provenance window's `ReceiptExpired` wrapper is crate-sealed
    // (`EnrollmentResponse::from_receipt_expired` is `pub(crate)`), so this
    // binding degrades the provenance window to the permanent lifetime
    // mapping: expired-receipt token replays answer `EnrollmentKnown`. This
    // is the closest crate-expressible row and is recorded as a residual gap
    // in the activation declaration.
    //
    // The gate reads the enrollment receipt's OWN deadline, fixed at enroll
    // commit: later attaches never re-open the generation-1 secret-bearing
    // receipt (secret receipts never outlive their signed TTL).
    let phase = if now < slot.enrollment_receipt_expires_at {
        EnrollmentTokenPhase::LiveReceipt {
            identity: ResolvedIdentity::<Digest, Digest, Digest>::Live(&slot.member),
            receipt: &slot.enrollment_receipt,
        }
    } else {
        EnrollmentTokenPhase::LifetimeMapping {
            identity: ResolvedIdentity::<Digest, Digest, Digest>::Live(&slot.member),
        }
    };
    let response = match lookup_enrollment(phase, &slot.binding, request) {
        EnrollmentLookupResult::EnrollmentKnown(value) => {
            EnrollmentResponse::enrollment_known(value)
        }
        EnrollmentLookupResult::Bound(_) => {
            EnrollmentResponse::bound(slot.enrollment_outcome.clone())
        }
        EnrollmentLookupResult::UnboundReceipt(_) => {
            EnrollmentResponse::unbound_receipt(slot.enrollment_outcome.clone())
        }
        EnrollmentLookupResult::Retired(_) => {
            return Err(StateError::invariant(
                "retired identity observed in a binding that mints no tombstones",
            ));
        }
        EnrollmentLookupResult::ReceiptExpired(_) => {
            return Err(StateError::invariant(
                "enrollment provenance phase is never constructed by this binding",
            ));
        }
        EnrollmentLookupResult::AuthorizedNew => {
            return Err(StateError::invariant(
                "mapped enrollment token classified as authorized-new",
            ));
        }
    };
    Ok(response.into_server_value())
}
