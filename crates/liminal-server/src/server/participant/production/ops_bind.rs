//! Enrollment and credential-attach arms of the production handler.
//!
//! Both arms classify through the shared lookup selectors, commit through
//! the crate's typed transitions, mint their shell events through the A3
//! aggregate commits, and answer through the request-bound response
//! authorities. No lifecycle outcome is constructed here.
//!
//! Error contract: any [`StateError`] leaves durable state untouched (nothing
//! is published before the append succeeds) but may have consumed in-memory
//! authority. The handler therefore discards the whole in-memory conversation
//! owner on error and cold-replays durable reality on the next touch — the
//! same crash-consistency model the aggregate barrier is built for.

use liminal_protocol::lifecycle::{
    AggregateOperationDecision, AllocatedParticipantSlot, AttachCommitParameters,
    AttachSecretProof, AttachedRecordPosition, BindingSlotDecision, BindingSlotOccupancy,
    BindingState, ClosureState, CredentialAttachLiveReceipt, CredentialAttachLookupResult,
    CredentialAttachProvenance, CredentialAttachTokenPhase, DetachCell, EnrollmentCommitParameters,
    EnrollmentFingerprint, EnrollmentLiveReceipt, EnrollmentLookupResult, EnrollmentTokenPhase,
    ParticipantSlotAllocatorProof, PresentedIdentity, ReceiptDeadlines, ResolvedIdentity,
    commit_attach, commit_enrollment, decide_attached_operation, decide_enrolled_operation,
    lookup_credential_attach, lookup_enrollment, select_credential_attach_binding_slot,
    select_enrollment_binding_slot,
};
use liminal_protocol::wire::{
    AttachBound, AttachEnvelope, AttachSecret, BindingEpoch, ConnectionIncarnation,
    CredentialAttachRequest, CredentialAttachResponse, EnrollBound, EnrollmentRequest,
    EnrollmentResponse, Generation, ReceiptExpiryReason, ServerValue,
};

use super::facts::{self, Digest};
use super::log::{
    StoredAttachAllocation, StoredAttachRequest, StoredEnrollmentAllocation,
    StoredEnrollmentRequest, StoredOperation,
};
use super::state::{ConversationAuthority, DurableAppend, Slot, StateError};

/// Connection-scoped and configured facts supplied to each operation.
#[derive(Clone, Copy, Debug)]
pub(super) struct OperationFacts {
    /// Durable incarnation of the receiving connection.
    pub(super) receiving_incarnation: ConnectionIncarnation,
    /// Admitted wall-clock read for deadline derivation and receipt phases.
    pub(super) now_ms: u64,
    /// Configured identity slots per enrolled participant.
    pub(super) identity_slots: u64,
    /// Configured secret-bearing receipt TTL.
    pub(super) attach_receipt_ttl_ms: u64,
    /// Configured non-secret provenance TTL.
    pub(super) receipt_provenance_ttl_ms: u64,
}

impl OperationFacts {
    fn deadlines(&self) -> Result<ReceiptDeadlines, StateError> {
        ReceiptDeadlines::try_from_ttls(
            self.now_ms,
            self.attach_receipt_ttl_ms,
            self.receipt_provenance_ttl_ms,
        )
        .map_err(|error| {
            StateError::invariant(format!("validated TTL configuration rejected: {error:?}"))
        })
    }
}

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
    /// Reports whether the receiving connection already owns a bound slot in
    /// this conversation, exposing only what the crate's stage-6 selector
    /// needs. Derived from binding-epoch authority; no side table exists.
    pub(super) fn binding_slot_occupancy(
        &self,
        receiving_incarnation: ConnectionIncarnation,
    ) -> BindingSlotOccupancy {
        for (participant_id, slot) in &self.slots {
            if let BindingState::Bound(active) = slot.binding {
                if active.binding_epoch.connection_incarnation == receiving_incarnation {
                    return BindingSlotOccupancy::Occupied {
                        participant_id: *participant_id,
                    };
                }
            }
        }
        BindingSlotOccupancy::Empty
    }

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
                attach_receipt: None,
                attach_outcome: None,
                attach_secret: AttachSecret::new(allocation.attach_secret),
                receipt_generation: Generation::ONE,
                exact_detach_token: None,
                receipt_expires_at: allocation.receipt_expires_at.get(),
                provenance_expires_at: allocation.provenance_expires_at.get(),
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

    /// Applies one credential-attach request end to end.
    ///
    /// Attach never creates a conversation: a fresh conversation id has no
    /// slots and classifies as `ParticipantUnknown` without any durable
    /// append.
    pub(super) fn apply_credential_attach(
        &mut self,
        request: &CredentialAttachRequest,
        operation_facts: &OperationFacts,
        appender: &dyn DurableAppend,
    ) -> Result<ServerValue, StateError> {
        let envelope = attach_envelope(request);
        let Some(slot) = self.slots.get(&request.participant_id) else {
            return Ok(CredentialAttachResponse::participant_unknown(envelope).into_server_value());
        };
        let secret_proof = if facts::constant_time_eq(
            &slot.attach_secret.into_bytes(),
            &request.attach_secret.into_bytes(),
        ) {
            AttachSecretProof::Verified
        } else {
            AttachSecretProof::Mismatch
        };
        let now = u128::from(operation_facts.now_ms);
        let provenance;
        let token_phase = match slot.attach_receipt.as_ref() {
            Some((token, receipt)) if *token == request.attach_attempt_token => {
                if now < slot.receipt_expires_at {
                    CredentialAttachTokenPhase::LiveReceipt {
                        identity: ResolvedIdentity::<Digest, Digest, Digest>::Live(&slot.member),
                        receipt,
                    }
                } else if now < slot.provenance_expires_at {
                    provenance = CredentialAttachProvenance::new(
                        slot.receipt_generation,
                        ReceiptExpiryReason::Deadline,
                    );
                    CredentialAttachTokenPhase::Provenance {
                        identity: ResolvedIdentity::<Digest, Digest, Digest>::Live(&slot.member),
                        provenance,
                    }
                } else {
                    CredentialAttachTokenPhase::AfterProvenance
                }
            }
            _ => CredentialAttachTokenPhase::NoMatch,
        };
        let lookup = lookup_credential_attach(
            token_phase,
            PresentedIdentity::Live(&slot.member),
            &slot.binding,
            request,
            secret_proof,
        );
        if !matches!(lookup, CredentialAttachLookupResult::AuthorizedFresh { .. }) {
            return credential_attach_refusal(&lookup, envelope, slot);
        }
        if let BindingSlotDecision::Respond(response) = select_credential_attach_binding_slot(
            request,
            self.binding_slot_occupancy(operation_facts.receiving_incarnation),
        ) {
            return Ok(response.into_server_value());
        }

        let deadlines = operation_facts.deadlines()?;
        // The rotation result: the new binding epoch carries the successor of
        // the verified current generation (the crate's ResultGeneration law).
        let next_generation = request
            .capability_generation
            .get()
            .checked_add(1)
            .and_then(Generation::new)
            .ok_or(StateError::AllocationExhausted {
                domain: "capability generation",
            })?;
        let (attached_order, attached_seq) = self.allocate_position()?;
        let allocation = StoredAttachAllocation {
            binding_epoch: BindingEpoch::new(
                operation_facts.receiving_incarnation,
                next_generation,
            )
            .into(),
            attach_secret: facts::mint_secret_bytes()?,
            attached_order,
            attached_seq,
            receipt_expires_at: deadlines.receipt_expires_at().into(),
            provenance_expires_at: deadlines.provenance_expires_at().into(),
        };
        let outcome = self.attach_commit(request, &allocation, CommitMode::Live(appender))?;
        Ok(CredentialAttachResponse::attach_bound(outcome).into_server_value())
    }

    /// Replays one committed attach entry from its stored inputs.
    pub(super) fn replay_attached(
        &mut self,
        request: StoredAttachRequest,
        allocation: &StoredAttachAllocation,
        stored_event: &[u8],
        sequence: u64,
    ) -> Result<(), StateError> {
        let request = request.to_request()?;
        self.attach_commit(
            &request,
            allocation,
            CommitMode::Replay {
                stored_event,
                sequence,
            },
        )?;
        Ok(())
    }

    /// Shared ordinary detached-attach commit core (live and replay paths).
    fn attach_commit(
        &mut self,
        request: &CredentialAttachRequest,
        allocation: &StoredAttachAllocation,
        mode: CommitMode<'_>,
    ) -> Result<AttachBound, StateError> {
        let (participant_id, mut slot) = self
            .slots
            .remove_entry(&request.participant_id)
            .ok_or_else(|| {
                StateError::invariant("attach commit requires an enrolled participant slot")
            })?;
        let closure_admission = ClosureState::Clear
            .ordinary_detached_attach_admission()
            .map_err(|error| {
                StateError::invariant(format!(
                    "clear closure refused detached attach admission: {error:?}"
                ))
            })?;
        let verified = slot
            .member
            .verify_detached_attach(
                slot.binding,
                closure_admission,
                request.clone(),
                AttachSecretProof::Verified,
                AttachCommitParameters {
                    binding: liminal_protocol::lifecycle::ActiveBinding {
                        participant_id: request.participant_id,
                        conversation_id: request.conversation_id,
                        binding_epoch: allocation.binding_epoch.to_epoch()?,
                    },
                    attach_secret: AttachSecret::new(allocation.attach_secret),
                    attached_position: AttachedRecordPosition::new(
                        allocation.attached_order,
                        allocation.attached_seq,
                    ),
                    receipt_expires_at: allocation.receipt_expires_at.get(),
                    provenance_expires_at: allocation.provenance_expires_at.get(),
                },
            )
            .map_err(|error| {
                StateError::invariant(format!("protocol attach verification failed: {error:?}"))
            })?;
        let committed = commit_attach(verified, slot.cell).map_err(|error| {
            StateError::invariant(format!("protocol attach transition failed: {error:?}"))
        })?;
        let shell = self.take_shell()?;
        let barrier = match decide_attached_operation(shell, committed) {
            AggregateOperationDecision::Commit(barrier) => barrier,
            AggregateOperationDecision::Refused(refusal) => {
                return Err(StateError::ShellRefused {
                    reason: refusal.reason(),
                });
            }
        };
        let make_operation = |event: Vec<u8>| StoredOperation::Attached {
            request: request.into(),
            secret_verified: true,
            allocation: *allocation,
            event,
        };
        let (shell, committed) =
            commit_through_barrier(barrier, mode, self.next_log_sequence, &make_operation)?;
        self.shell = Some(shell);
        self.advance_log_head()?;
        let outcome = committed.outcome.clone();
        slot.member = committed.member;
        slot.binding = committed.binding_state;
        slot.cell = committed.detach_cell;
        slot.attach_secret = AttachSecret::new(allocation.attach_secret);
        slot.attach_receipt = Some((
            request.attach_attempt_token,
            CredentialAttachLiveReceipt::from_commit(outcome.clone()),
        ));
        slot.attach_outcome = Some(committed.outcome);
        slot.receipt_generation = request.capability_generation;
        slot.receipt_expires_at = allocation.receipt_expires_at.get();
        slot.provenance_expires_at = allocation.provenance_expires_at.get();
        self.slots.insert(participant_id, slot);
        self.observe_replayed_position(allocation.attached_order, allocation.attached_seq);
        Ok(outcome)
    }

    /// Advances the position allocators past a replayed entry's positions.
    fn observe_replayed_position(&mut self, order: u64, seq: u64) {
        self.next_order = self.next_order.max(order.saturating_add(1));
        self.next_seq = self.next_seq.max(seq.saturating_add(1));
    }
}

/// One barrier resolution mode: live durable append or replay byte-check.
#[derive(Clone, Copy)]
pub(super) enum CommitMode<'a> {
    /// Append the operation at the optimistic head, then commit.
    Live(&'a dyn DurableAppend),
    /// Cross-check re-minted canonical bytes against the stored entry.
    Replay {
        /// Canonical event bytes read from the durable entry.
        stored_event: &'a [u8],
        /// Durable log sequence of the entry (for drift diagnostics).
        sequence: u64,
    },
}

/// Resolves one pending aggregate barrier through the selected mode.
pub(super) fn commit_through_barrier<T>(
    barrier: liminal_protocol::lifecycle::AggregateOperationCommit<T>,
    mode: CommitMode<'_>,
    next_log_sequence: u64,
    make_operation: &dyn Fn(Vec<u8>) -> StoredOperation,
) -> Result<(liminal_protocol::lifecycle::ParticipantConversation, T), StateError> {
    let event = barrier.event().encode_canonical();
    match mode {
        CommitMode::Live(appender) => {
            let operation = make_operation(event);
            appender.append(&operation, next_log_sequence)?;
        }
        CommitMode::Replay {
            stored_event,
            sequence,
        } => {
            if event != stored_event {
                return Err(StateError::ReplayedEventDrift { sequence });
            }
        }
    }
    Ok(barrier.commit())
}

/// Builds the echo envelope of one credential-attach request.
const fn attach_envelope(request: &CredentialAttachRequest) -> AttachEnvelope {
    AttachEnvelope {
        conversation_id: request.conversation_id,
        participant_id: request.participant_id,
        capability_generation: request.capability_generation,
        attach_attempt_token: request.attach_attempt_token,
        accept_marker_delivery_seq: request.accept_marker_delivery_seq,
    }
}

/// Maps a non-authorized credential-attach lookup onto its bound response.
fn credential_attach_refusal(
    lookup: &CredentialAttachLookupResult<'_, Digest>,
    envelope: AttachEnvelope,
    slot: &Slot,
) -> Result<ServerValue, StateError> {
    let response = match lookup {
        CredentialAttachLookupResult::ParticipantUnknown(_) => {
            CredentialAttachResponse::participant_unknown(envelope)
        }
        CredentialAttachLookupResult::StaleAuthority(_) => {
            CredentialAttachResponse::stale_authority(envelope, slot.member.generation())
        }
        CredentialAttachLookupResult::AttemptTokenBodyConflict(_) => {
            // The conflict kind is re-derived from the same facts the lookup
            // compared: generation first, then the marker sequence.
            let conflict = if envelope.capability_generation == slot.receipt_generation {
                liminal_protocol::wire::AttemptConflict::MarkerDeliverySequence
            } else {
                liminal_protocol::wire::AttemptConflict::Generation
            };
            CredentialAttachResponse::attempt_token_body_conflict(&envelope, conflict)
        }
        CredentialAttachLookupResult::Bound(_) => {
            let outcome = slot.attach_outcome.clone().ok_or_else(|| {
                StateError::invariant("attach receipt replay without a stored receipt")
            })?;
            CredentialAttachResponse::bound(outcome)
        }
        CredentialAttachLookupResult::UnboundReceipt(_) => {
            let outcome = slot.attach_outcome.clone().ok_or_else(|| {
                StateError::invariant("attach receipt replay without a stored receipt")
            })?;
            CredentialAttachResponse::unbound_receipt(outcome)
        }
        CredentialAttachLookupResult::ReceiptExpired(_) => {
            CredentialAttachResponse::receipt_expired(
                &envelope,
                slot.receipt_generation,
                slot.member.generation(),
                ReceiptExpiryReason::Deadline,
            )
        }
        CredentialAttachLookupResult::StaleOrUnknownReceipt(value) => {
            CredentialAttachResponse::stale_or_unknown_receipt(value.clone())
        }
        CredentialAttachLookupResult::Retired(_) => {
            return Err(StateError::invariant(
                "retired identity observed in a binding that mints no tombstones",
            ));
        }
        CredentialAttachLookupResult::AuthorizedFresh { .. } => {
            return Err(StateError::invariant(
                "authorized attach routed through the refusal mapper",
            ));
        }
    };
    Ok(response.into_server_value())
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
    let phase = if now < slot.receipt_expires_at {
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
